"""Visibility: ``Client.list_workflows`` / ``count_workflows`` (DESIGN §6.2).

Exercises the query subset end-to-end against Postgres: filtering by workflow
type, ExecutionStatus (clean + the ERROR-family post-filter), WorkflowId
(exact + STARTS_WITH), search-attribute equality (the GIN-indexed containment
path), AND combinations, plus count, limit, and offset-token pagination.

Read-only over committed DBOS status — no interpreter/checkpoint surface — so
no kill-and-recover test is warranted here (cf. CLAUDE.md).
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos._internal.visibility import VisibilityQueryError
from temporal_dbos.client import (
    Client,
    WorkflowExecution,
    WorkflowExecutionAsyncIterator,
    WorkflowExecutionCount,
    WorkflowExecutionStatus,
    WorkflowFailureError,
)
from temporal_dbos.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "list-wf-tq"
KW = SearchAttributeKey.for_keyword("CustomKeyword")


@workflow.defn
class Completer:
    @workflow.run
    async def run(self, value: str) -> str:
        return value


@workflow.defn
class Failer:
    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("boom")


@workflow.defn
class Holder:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[Completer, Failer, Holder],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def _collect(it: WorkflowExecutionAsyncIterator) -> List[WorkflowExecution]:
    return [e async for e in it]


def _sa(value: str) -> TypedSearchAttributes:
    return TypedSearchAttributes([SearchAttributePair(KW, value)])


async def test_list_by_workflow_type() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        await client.execute_workflow(
            Completer.run, "b", id="c2", task_queue=TASK_QUEUE
        )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        rows = await _collect(client.list_workflows("WorkflowType = 'Completer'"))
        assert {r.id for r in rows} == {"c1", "c2"}
        assert all(r.workflow_type == "Completer" for r in rows)

        await holder.signal(Holder.finish)
        await holder.result()


async def test_list_by_type_in() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        rows = await _collect(
            client.list_workflows("WorkflowType IN ('Completer', 'Holder')")
        )
        assert {r.id for r in rows} == {"c1", "h1"}

        await holder.signal(Holder.finish)
        await holder.result()


async def test_list_by_status_running_and_completed() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        running = await _collect(client.list_workflows("ExecutionStatus = 'Running'"))
        assert "h1" in {r.id for r in running}
        assert all(r.status == WorkflowExecutionStatus.RUNNING for r in running)

        completed = await _collect(
            client.list_workflows("ExecutionStatus = 'Completed'")
        )
        assert "c1" in {r.id for r in completed}
        assert all(r.status == WorkflowExecutionStatus.COMPLETED for r in completed)

        await holder.signal(Holder.finish)
        await holder.result()


async def test_list_by_status_failed_post_filter() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "ok", id="c1", task_queue=TASK_QUEUE
        )
        with pytest.raises(WorkflowFailureError):
            await client.execute_workflow(Failer.run, id="f1", task_queue=TASK_QUEUE)

        failed = await _collect(client.list_workflows("ExecutionStatus = 'Failed'"))
        ids = {r.id for r in failed}
        # The failure is FAILED; the success must NOT appear (the post-filter
        # keeps only the ERROR rows whose marker maps to FAILED).
        assert "f1" in ids
        assert "c1" not in ids
        assert all(r.status == WorkflowExecutionStatus.FAILED for r in failed)


async def test_list_by_workflow_id_and_prefix() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="order-1", task_queue=TASK_QUEUE
        )
        await client.execute_workflow(
            Completer.run, "b", id="order-2", task_queue=TASK_QUEUE
        )
        await client.execute_workflow(
            Completer.run, "c", id="other", task_queue=TASK_QUEUE
        )

        exact = await _collect(client.list_workflows("WorkflowId = 'order-1'"))
        assert {r.id for r in exact} == {"order-1"}

        prefixed = await _collect(
            client.list_workflows("WorkflowId STARTS_WITH 'order-'")
        )
        assert {r.id for r in prefixed} == {"order-1", "order-2"}


async def test_list_by_search_attribute_containment() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run,
            "a",
            id="c1",
            task_queue=TASK_QUEUE,
            search_attributes=_sa("vip"),
        )
        await client.execute_workflow(
            Completer.run,
            "b",
            id="c2",
            task_queue=TASK_QUEUE,
            search_attributes=_sa("normal"),
        )

        rows = await _collect(client.list_workflows("CustomKeyword = 'vip'"))
        assert {r.id for r in rows} == {"c1"}


async def test_list_and_combination() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run,
            "a",
            id="c1",
            task_queue=TASK_QUEUE,
            search_attributes=_sa("vip"),
        )
        await client.execute_workflow(
            Completer.run,
            "b",
            id="c2",
            task_queue=TASK_QUEUE,
            search_attributes=_sa("vip"),
        )
        await client.execute_workflow(
            Completer.run,
            "c",
            id="c3",
            task_queue=TASK_QUEUE,
            search_attributes=_sa("no"),
        )

        rows = await _collect(
            client.list_workflows(
                "WorkflowType = 'Completer' AND ExecutionStatus = 'Completed' "
                "AND CustomKeyword = 'vip'"
            )
        )
        assert {r.id for r in rows} == {"c1", "c2"}


async def test_count_workflows() -> None:
    async with _env() as client:
        for i in range(3):
            await client.execute_workflow(
                Completer.run, str(i), id=f"c{i}", task_queue=TASK_QUEUE
            )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        assert (await client.count_workflows("WorkflowType = 'Completer'")).count == 3
        assert (await client.count_workflows("WorkflowType = 'Holder'")).count == 1
        # Clean (non-error) status counts go through the aggregate operator.
        running = await client.count_workflows("ExecutionStatus = 'Running'")
        assert running.count >= 1
        assert running.groups == []
        assert (
            await client.count_workflows("ExecutionStatus = 'Completed'")
        ).count == 3

        await holder.signal(Holder.finish)
        await holder.result()


def _groups(count: WorkflowExecutionCount) -> Dict[Any, Optional[int]]:
    return {g.group_values[0]: g.count for g in count.groups}


async def test_count_group_by_execution_status_rejected() -> None:
    # DBOS lumps Failed/Canceled/TimedOut/ContinuedAsNew under one ERROR status,
    # so a faithful GROUP BY ExecutionStatus can't be computed by the aggregate
    # operator alone — count_workflows fails it rather than scanning rows.
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        with pytest.raises(VisibilityQueryError):
            await client.count_workflows("GROUP BY ExecutionStatus")


async def test_count_rejects_unaggregatable_filters() -> None:
    # Filters DBOS's aggregate operator can't express are rejected, not scanned.
    async with _env() as client:
        await client.execute_workflow(
            Completer.run,
            "a",
            id="c1",
            task_queue=TASK_QUEUE,
            search_attributes=_sa("vip"),
        )
        with pytest.raises(VisibilityQueryError):
            await client.count_workflows("CustomKeyword = 'vip'")  # search attribute
        with pytest.raises(VisibilityQueryError):
            await client.count_workflows("WorkflowId = 'c1'")  # exact id
        with pytest.raises(VisibilityQueryError):
            await client.count_workflows("WorkflowType != 'Completer'")  # negation
        with pytest.raises(VisibilityQueryError):
            await client.count_workflows("ExecutionStatus = 'Failed'")  # error-family


async def test_count_group_by_workflow_type() -> None:
    async with _env() as client:
        for i in range(2):
            await client.execute_workflow(
                Completer.run, str(i), id=f"c{i}", task_queue=TASK_QUEUE
            )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        count = await client.count_workflows("GROUP BY WorkflowType")
        groups = _groups(count)
        assert groups.get("Completer") == 2
        assert groups.get("Holder") == 1
        assert count.count == 3

        await holder.signal(Holder.finish)
        await holder.result()


async def test_limit() -> None:
    async with _env() as client:
        for i in range(5):
            await client.execute_workflow(
                Completer.run, str(i), id=f"c{i}", task_queue=TASK_QUEUE
            )
        rows = await _collect(
            client.list_workflows("WorkflowType = 'Completer'", limit=3)
        )
        assert len(rows) == 3


async def test_pagination_with_next_page_token() -> None:
    async with _env() as client:
        for i in range(5):
            await client.execute_workflow(
                Completer.run, str(i), id=f"c{i}", task_queue=TASK_QUEUE
            )

        # Page 1.
        it1 = client.list_workflows("WorkflowType = 'Completer'", page_size=2)
        await it1.fetch_next_page()
        assert it1.current_page is not None and len(it1.current_page) == 2
        token = it1.next_page_token
        assert token is not None
        page1_ids = {e.id for e in it1.current_page}

        # Page 2 from the token: a fresh iterator resuming at the saved offset.
        it2 = client.list_workflows(
            "WorkflowType = 'Completer'", page_size=2, next_page_token=token
        )
        await it2.fetch_next_page()
        assert it2.current_page is not None
        page2_ids = {e.id for e in it2.current_page}
        assert page1_ids.isdisjoint(page2_ids)

        # Full iteration still yields all five exactly once.
        everything = await _collect(
            client.list_workflows("WorkflowType = 'Completer'", page_size=2)
        )
        assert {e.id for e in everything} == {f"c{i}" for i in range(5)}


async def test_bad_query_raises_on_first_iteration() -> None:
    async with _env() as client:
        # list_workflows is lazy: the parser error (a ValueError subclass)
        # surfaces on first __anext__, not at the list_workflows() call.
        it = client.list_workflows("WorkflowType = 'A' OR WorkflowType = 'B'")
        with pytest.raises(ValueError):
            await _collect(it)
