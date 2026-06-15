"""Continue-as-new (Phase 3): chain hops, result-following, status mapping,
message carryover, run-timeout carry, and child chains.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Awaitable, Callable, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos._internal import inbox
from temporal_dbos.client import (
    Client,
    WorkflowContinuedAsNewError,
    WorkflowExecutionStatus,
    WorkflowFailureError,
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
class HopChild:
    def __init__(self) -> None:
        self.hop = False

    @workflow.signal
    def hop_now(self) -> None:
        self.hop = True

    @workflow.run
    async def run(self, hopped: bool) -> str:
        if not hopped:
            await workflow.wait_condition(lambda: self.hop)
            workflow.continue_as_new(True)
        await workflow.wait_condition(lambda: False)  # park until swept
        return "unreachable"


@workflow.defn
class SweepParent:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> None:
        await workflow.start_child_workflow(HopChild.run, False, id="sweep-child")
        await workflow.wait_condition(lambda: self.done)


@workflow.defn
class CancelHopWorkflow:
    @workflow.run
    async def run(self, hopped: bool) -> str:
        if hopped:
            await workflow.wait_condition(lambda: False)
            return "unreachable"
        try:
            await workflow.wait_condition(lambda: False)
        except asyncio.CancelledError:
            # Cleanup-then-continue: the outstanding cancel request must
            # carry over to the new run (Temporal semantics).
            workflow.continue_as_new(True)
        return "uncancelled"


@workflow.defn
class HandlerHopWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def hop(self, payload: str) -> None:
        workflow.continue_as_new(payload)

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self, carried: str) -> str:
        await workflow.wait_condition(lambda: self.done)
        return carried


@workflow.defn
class TypeSwitchWorkflow:
    # Annotated with the chain's eventual result type (LoopingWorkflow returns
    # List[str]); the client infers result_type from this run signature, and a
    # mismatched annotation would now fail to decode — matching temporalio's
    # type-faithful result conversion rather than pickle's exact-object pass.
    @workflow.run
    async def run(self) -> List[str]:
        workflow.continue_as_new(args=[["switched"], 0], workflow=LoopingWorkflow.run)


@workflow.defn
class CanThenChildWorkflow:
    @workflow.run
    async def run(self, hopped: bool) -> List[Any]:
        if not hopped:
            workflow.continue_as_new(True)
        # This run is --r1: its auto child id embeds "--r1" (the shape that
        # would mis-parse without the digit guard), and the child itself
        # continues as new, extending ITS OWN chain.
        handle = await workflow.start_child_workflow(
            LinkProbeWorkflow.run, args=[[], 1]
        )
        links = await handle
        return [links, handle.id]


@workflow.defn
class BadChildIdWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.start_child_workflow(
            LinkProbeWorkflow.run, args=[[], 0], id="explicit--r1"
        )


@workflow.defn
class AutoHopChild:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish_child(self) -> None:
        self.done = True

    @workflow.run
    async def run(self, hopped: bool) -> str:
        if not hopped:
            workflow.continue_as_new(True)
        await workflow.wait_condition(lambda: self.done)
        return "child-done"


@workflow.defn
class SignalHoppedChildParent:
    @workflow.run
    async def run(self) -> str:
        handle = await workflow.start_child_workflow(AutoHopChild.run, False)
        await workflow.sleep(0.5)  # let the child hop to --r1
        await handle.signal(AutoHopChild.finish_child)
        result: str = await handle
        return result


@workflow.defn
class AbandonedHoppedChildParent:
    @workflow.run
    async def run(self) -> str:
        handle = await workflow.start_child_workflow(
            AutoHopChild.run,
            False,
            id="abandoned-hop-child",
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )
        # Awaiting the child is what routes the parent's cancellation into
        # the child future (the in-flight cancellation sweep under test);
        # ABANDON keeps the close sweep out of the picture.
        result: str = await handle
        return result


@workflow.defn
class CanParent:
    @workflow.run
    async def run(self) -> List[str]:
        result: List[str] = await workflow.execute_child_workflow(
            LoopingWorkflow.run, args=[[], 2], id="can-child"
        )
        return result


@workflow.defn
class SlowChainWorkflow:
    @workflow.run
    async def run(self, rounds: int) -> str:
        await workflow.sleep(1.0)
        if rounds == 0:
            return "slow-chain-done"
        workflow.continue_as_new(rounds - 1)


@workflow.defn
class TimeoutOverrideChain:
    @workflow.run
    async def run(self, hopped: bool) -> Optional[float]:
        if not hopped:
            workflow.continue_as_new(True, run_timeout=timedelta(seconds=30))
        run_timeout = workflow.info().run_timeout
        return run_timeout.total_seconds() if run_timeout is not None else None


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            SlowChainWorkflow,
            TimeoutOverrideChain,
            LoopingWorkflow,
            CarryoverWorkflow,
            CanParent,
            LinkProbeWorkflow,
            LinkParent,
            UpdateCarryWorkflow,
            HopChild,
            SweepParent,
            CancelHopWorkflow,
            HandlerHopWorkflow,
            TypeSwitchWorkflow,
            CanThenChildWorkflow,
            BadChildIdWorkflow,
            AutoHopChild,
            SignalHoppedChildParent,
            AbandonedHoppedChildParent,
        ],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def test_run_timeout_is_per_run_across_continue_as_new() -> None:
    """Each CAN run gets a fresh run_timeout: three 1s runs complete under a
    2s per-run budget even though the chain's total (3s+) exceeds it.

    Regression: without explicit re-application on the hop, DBOS propagates
    run 0's *absolute* deadline to its successors, so the chain would be
    natively killed mid-way (TERMINATED). Temporal applies run_timeout per
    run.
    """
    async with _env() as client:
        result = await client.execute_workflow(
            SlowChainWorkflow.run,
            2,
            id="slow-chain",
            task_queue=TASK_QUEUE,
            run_timeout=timedelta(seconds=2),
        )
        assert result == "slow-chain-done"


async def test_continue_as_new_run_timeout_override() -> None:
    """continue_as_new(run_timeout=...) overrides the carried per-run
    timeout for the new run, visible in its workflow.info()."""
    async with _env() as client:
        result = await client.execute_workflow(
            TimeoutOverrideChain.run,
            False,
            id="timeout-override",
            task_queue=TASK_QUEUE,
            run_timeout=timedelta(seconds=5),
        )
        assert result == 30.0


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


async def _wait_for(
    predicate: "Callable[[], Awaitable[bool]]", timeout: float = 20.0
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition not met"
        await asyncio.sleep(0.1)


async def _exists(client: Client, dbos_id: str) -> bool:
    statuses = await client._dbos_client.list_workflows_async(workflow_ids=[dbos_id])
    return bool(statuses)


async def _status_of(client: Client, dbos_id: str) -> WorkflowExecutionStatus:
    from temporal_dbos._internal import ids as _ids

    handle = client.get_workflow_handle(_ids.parse_run(dbos_id)[0], run_id=dbos_id)
    status = (await handle.describe()).status
    assert status is not None
    return status


async def test_parent_close_policy_follows_child_chain() -> None:
    """ParentClosePolicy applies to the child's *chain*: a child that
    continued as new must have its current run terminated when the parent
    closes, not its long-closed first run."""
    async with _env() as client:
        parent = await client.start_workflow(
            SweepParent.run, id="sweep-parent", task_queue=TASK_QUEUE
        )
        child = client.get_workflow_handle("sweep-child")
        await _wait_for(lambda: _exists(client, "sweep-child"))
        await child.signal(HopChild.hop_now)
        await _wait_for(lambda: _exists(client, "sweep-child--r1"))
        await parent.signal(SweepParent.finish)
        assert await parent.result() is None

        async def child_terminated() -> bool:
            current = await _status_of(client, "sweep-child--r1")
            return current == WorkflowExecutionStatus.TERMINATED

        await _wait_for(child_terminated)


async def test_describe_parent_id_excludes_continuation_links() -> None:
    """describe().parent_id is a real (cross-chain) parent only: a
    continue-as-new run's same-chain DBOS link must not appear as a
    parent."""
    async with _env() as client:
        await client.execute_workflow(
            LinkProbeWorkflow.run, args=[[], 1], id="pid-wf", task_queue=TASK_QUEUE
        )
        hopped = client.get_workflow_handle("pid-wf", run_id="pid-wf--r1")
        assert (await hopped.describe()).parent_id is None

        await client.execute_workflow(
            LinkParent.run, id="pid-parent", task_queue=TASK_QUEUE
        )
        child = client.get_workflow_handle("link-child")
        assert (await child.describe()).parent_id == "pid-parent"


async def test_cancel_carries_across_can() -> None:
    """A cancel request outstanding when the run continues as new carries
    over: the new run starts already-cancelled (Temporal semantics)."""
    async with _env() as client:
        handle = await client.start_workflow(
            CancelHopWorkflow.run, False, id="cancel-hop", task_queue=TASK_QUEUE
        )
        await _wait_for(lambda: _exists(client, "cancel-hop"))
        await handle.cancel()
        with pytest.raises(Exception) as exc_info:
            await handle.result()
        assert "Workflow execution failed" in str(exc_info.value)
        hopped = client.get_workflow_handle("cancel-hop", run_id="cancel-hop--r1")
        assert (await hopped.describe()).status == WorkflowExecutionStatus.CANCELED


async def test_continue_as_new_from_signal_handler() -> None:
    """Signal handlers may initiate continue-as-new (as in Temporal); the
    remaining inbox (here: finish) rides carryover to the new run."""
    async with _env() as client:
        handle = await client.start_workflow(
            HandlerHopWorkflow.run, "start", id="handler-hop", task_queue=TASK_QUEUE
        )
        await handle.signal(HandlerHopWorkflow.hop, "hopped")
        await handle.signal(HandlerHopWorkflow.finish)
        assert await handle.result() == "hopped"


async def test_continue_as_new_to_different_workflow() -> None:
    """continue_as_new(workflow=...) switches the chain to another workflow
    type, as in Temporal."""
    async with _env() as client:
        result = await client.execute_workflow(
            TypeSwitchWorkflow.run, id="type-switch", task_queue=TASK_QUEUE
        )
        assert result == ["switched"]
        hopped = client.get_workflow_handle("type-switch", run_id="type-switch--r1")
        assert (await hopped.describe()).workflow_type == "LoopingWorkflow"


async def test_children_of_continued_runs() -> None:
    """A child of a CAN-created run gets an auto id embedding the parent's
    chain suffix; it must still be a *standalone* chain base: no false
    continuation link, a real parent in describe, and its own CAN extends
    its own chain (not the parent's)."""
    async with _env() as client:
        links, child_id = await client.execute_workflow(
            CanThenChildWorkflow.run, False, id="can-host", task_queue=TASK_QUEUE
        )
        assert "--r1_" in child_id  # the colliding shape was exercised
        # The child CANed once: run 0 has no continuation link, run 1 links
        # to run 0 of the CHILD's chain (not the host's).
        assert links == [None, child_id]
        child_run0 = client.get_workflow_handle(child_id, run_id=child_id)
        description = await child_run0.describe()
        assert description.parent_id == "can-host--r1"
        # The child's chain extended under its own id.
        hopped = client.get_workflow_handle(child_id)
        assert (await hopped.describe()).run_id == f"{child_id}--r1"


async def test_explicit_child_id_with_separator_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit child ids obey the same `--r` reservation as client-side
    starts (auto ids are exempt by construction)."""
    monkeypatch.setenv("TEMPORAL_DBOS_FAIL_FAST", "1")
    async with _env() as client:
        handle = await client.start_workflow(
            BadChildIdWorkflow.run, id="bad-child-id", task_queue=TASK_QUEUE
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        assert "--r" in str(exc_info.value.cause)


async def test_child_handle_signal_follows_chain() -> None:
    """Signals through a ChildWorkflowHandle reach the child's *current*
    run after it continues as new (and the parent's child-result wait
    follows the chain to the final result)."""
    async with _env() as client:
        result = await client.execute_workflow(
            SignalHoppedChildParent.run, id="sig-hop-parent", task_queue=TASK_QUEUE
        )
        assert result == "child-done"


async def test_unwind_child_cancel_follows_chain() -> None:
    """A parent's cancellation unwind delivers the child's cooperative
    cancel to the child's *current* run, not the run it originally started
    (ABANDON close policy isolates this path from the close sweep)."""
    async with _env() as client:
        parent = await client.start_workflow(
            AbandonedHoppedChildParent.run, id="unwind-parent", task_queue=TASK_QUEUE
        )

        async def child_hopped() -> bool:
            return await _exists(client, "abandoned-hop-child--r1")

        await _wait_for(child_hopped)
        await parent.cancel()
        with pytest.raises(WorkflowFailureError):
            await parent.result()

        async def child_cancelled() -> bool:
            return (
                await _status_of(client, "abandoned-hop-child--r1")
            ) == WorkflowExecutionStatus.CANCELED

        await _wait_for(child_cancelled)


async def test_terminate_follows_can_and_never_clobbers() -> None:
    """terminate() on a run that already continued as new raises instead of
    clobbering its recorded marker status; an unbound terminate resolves and
    kills the chain's live run."""
    async with _env() as client:
        handle = await client.start_workflow(
            CarryoverWorkflow.run, [], id="term-can", task_queue=TASK_QUEUE
        )
        await handle.signal(CarryoverWorkflow.hop_now)

        async def hopped() -> bool:
            return await _exists(client, "term-can--r1")

        await _wait_for(hopped)

        async def first_run_closed() -> bool:
            # CAN enqueues the successor BEFORE recording its own marker;
            # wait until run 0 is actually closed (terminating it in that
            # window is legal — the convergence loop follows the successor).
            return (
                await _status_of(client, "term-can")
            ) == WorkflowExecutionStatus.CONTINUED_AS_NEW

        await _wait_for(first_run_closed)
        # Bound terminate on the closed first run: refused, marker intact.
        first = client.get_workflow_handle("term-can", run_id="term-can")
        with pytest.raises(RuntimeError, match="already closed"):
            await first.terminate()
        assert (await first.describe()).status == (
            WorkflowExecutionStatus.CONTINUED_AS_NEW
        )
        # Unbound terminate resolves the live run and kills it.
        await handle.terminate()
        assert (
            await _status_of(client, "term-can--r1")
        ) == WorkflowExecutionStatus.TERMINATED
