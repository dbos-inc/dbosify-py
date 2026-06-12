"""Continue-as-new (Phase 3): chain hops, result-following, status mapping,
message carryover, and child chains.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos._internal import inbox
from temporal_dbos.client import (
    Client,
    WorkflowContinuedAsNewError,
    WorkflowExecutionStatus,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "can-tq"


@workflow.defn
class LoopingWorkflow:
    @workflow.run
    async def run(self, items: List[str], rounds: int) -> List[str]:
        if rounds == 0:
            return items
        items.append(f"r{rounds}")
        workflow.continue_as_new(args=[items, rounds - 1])


@workflow.defn
class CarryoverWorkflow:
    def __init__(self) -> None:
        self.seen: List[str] = []
        self.hop = False
        self.done = False

    @workflow.signal
    def data(self, x: str) -> None:
        self.seen.append(x)

    @workflow.signal
    def hop_now(self) -> None:
        self.hop = True

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self, carried: List[str]) -> List[str]:
        self.seen = carried + self.seen
        await workflow.wait_condition(lambda: self.hop or self.done)
        if self.hop:
            workflow.continue_as_new(self.seen)
        return self.seen


@workflow.defn
class UpdateCarryWorkflow:
    def __init__(self) -> None:
        self.total = 0
        self.hop = False
        self.done = False

    @workflow.signal
    def hop_now(self) -> None:
        self.hop = True

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.update
    def add(self, n: int) -> int:
        self.total += n
        return self.total

    @workflow.run
    async def run(self, carried: int) -> int:
        self.total += carried
        await workflow.wait_condition(lambda: self.hop or self.done)
        if self.hop:
            workflow.continue_as_new(self.total)
        return self.total


@workflow.defn
class LinkProbeWorkflow:
    @workflow.run
    async def run(self, links: List[Optional[str]], rounds: int) -> List[Optional[str]]:
        links.append(workflow.info().continued_run_id)
        if rounds == 0:
            return links
        workflow.continue_as_new(args=[links, rounds - 1])


@workflow.defn
class LinkParent:
    @workflow.run
    async def run(self) -> List[Optional[str]]:
        result: List[Optional[str]] = await workflow.execute_child_workflow(
            LinkProbeWorkflow.run, args=[[], 0], id="link-child"
        )
        return result


@workflow.defn
class CanParent:
    @workflow.run
    async def run(self) -> List[str]:
        result: List[str] = await workflow.execute_child_workflow(
            LoopingWorkflow.run, args=[[], 2], id="can-child"
        )
        return result


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            LoopingWorkflow,
            CarryoverWorkflow,
            CanParent,
            LinkProbeWorkflow,
            LinkParent,
            UpdateCarryWorkflow,
        ],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def test_continue_as_new_chain() -> None:
    """The chain runs to completion; result() follows runs by default,
    raises WorkflowContinuedAsNewError with follow_runs=False, and the
    closed runs describe as CONTINUED_AS_NEW."""
    async with _env() as client:
        result = await client.execute_workflow(
            LoopingWorkflow.run, args=[[], 3], id="loop-wf", task_queue=TASK_QUEUE
        )
        assert result == ["r3", "r2", "r1"]

        first = client.get_workflow_handle("loop-wf", run_id="loop-wf")
        assert (await first.describe()).status == (
            WorkflowExecutionStatus.CONTINUED_AS_NEW
        )
        with pytest.raises(WorkflowContinuedAsNewError) as exc_info:
            await first.result(follow_runs=False)
        assert exc_info.value.new_execution_run_id == "loop-wf--r1"

        # The unbound handle resolves to the final run of the chain.
        latest = client.get_workflow_handle("loop-wf")
        description = await latest.describe()
        assert description.run_id == "loop-wf--r3"
        assert description.status == WorkflowExecutionStatus.COMPLETED


async def test_continue_as_new_carries_over_messages() -> None:
    """Signals not yet consumed when the run continues as new are forwarded
    to the new run (Temporal's carryover), preserving order."""
    async with _env() as client:
        handle = await client.start_workflow(
            CarryoverWorkflow.run, [], id="carry-wf", task_queue=TASK_QUEUE
        )
        for x in ["d1", "d2", "d3"]:
            await handle.signal(CarryoverWorkflow.data, x)
        # hop_now triggers CAN; everything after it is still in the inbox
        # when the old run closes and must be forwarded.
        await handle.signal(CarryoverWorkflow.hop_now)
        for x in ["d4", "d5"]:
            await handle.signal(CarryoverWorkflow.data, x)
        await handle.signal(CarryoverWorkflow.finish)
        assert await handle.result() == ["d1", "d2", "d3", "d4", "d5"]


async def test_child_workflow_continues_as_new() -> None:
    """A child that continues as new resolves on the parent with the final
    run's result (the child-result step follows the chain)."""
    async with _env() as client:
        result = await client.execute_workflow(
            CanParent.run, id="can-parent", task_queue=TASK_QUEUE
        )
        assert result == ["r2", "r1"]


async def test_continued_run_id() -> None:
    """continued_run_id is the previous run for continuation links only:
    None on a first run, the prior run id across continue-as-new hops, None
    again for id-reuse runs and for child workflows (whose DBOS parent link
    points outside the chain)."""
    async with _env() as client:
        chain = await client.execute_workflow(
            LinkProbeWorkflow.run, args=[[], 2], id="link-wf", task_queue=TASK_QUEUE
        )
        assert chain == [None, "link-wf", "link-wf--r1"]

        # Reuse-after-close creates run --r1 with NO continuation link.
        await client.execute_workflow(
            LinkProbeWorkflow.run, args=[[], 0], id="reuse-link", task_queue=TASK_QUEUE
        )
        reused = await client.execute_workflow(
            LinkProbeWorkflow.run, args=[[], 0], id="reuse-link", task_queue=TASK_QUEUE
        )
        assert reused == [None]

        # A child's DBOS parent link points at a different chain: not a
        # continuation.
        assert await client.execute_workflow(
            LinkParent.run, id="link-parent", task_queue=TASK_QUEUE
        ) == [None]


async def test_update_forwarded_across_can() -> None:
    """An update still unconsumed when its target run continues as new is
    forwarded to and executed by the new run — and the client still gets
    the result, because reply waits walk the chain (the new run writes
    acceptance/result events under its own id, not the one the client
    originally targeted)."""
    async with _env() as client:
        handle = await client.start_workflow(
            UpdateCarryWorkflow.run, 0, id="upd-carry-wf", task_queue=TASK_QUEUE
        )
        # FIFO: the hop is consumed first, the run stops consuming, and the
        # update (sent right behind it) rides carryover to run --r1.
        await handle.signal(UpdateCarryWorkflow.hop_now)
        assert (
            await handle.execute_update(UpdateCarryWorkflow.add, 5, id="carried-upd")
            == 5
        )
        # Prove the forward actually happened: the result event lives on the
        # new run, and the originally-targeted run never wrote one.
        dbos_client = client._dbos_client
        key = inbox.update_result_key("carried-upd")
        assert await dbos_client.get_event_async("upd-carry-wf--r1", key, 1.0)
        assert await dbos_client.get_event_async("upd-carry-wf", key, 0) is None
        await handle.signal(UpdateCarryWorkflow.finish)
        assert await handle.result() == 5
