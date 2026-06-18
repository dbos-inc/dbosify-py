"""Visibility: ``Client.list_workflows`` / ``count_workflows`` (DESIGN §6.2).

Exercises the query subset end-to-end against Postgres: filtering by workflow
type, ExecutionStatus (clean + the ERROR-family post-filter), WorkflowId
(exact + STARTS_WITH), search-attribute equality (the GIN-indexed containment
path), AND combinations, plus count, limit, and offset-token pagination.

Read-only over committed DBOS status — no interpreter/checkpoint surface — so
no kill-and-recover test is warranted here (cf. CLAUDE.md).
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos._internal.visibility import VisibilityQueryError
from temporal_dbos.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
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
from tests.harness import retry_until_success_async

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "list-wf-tq"
KW = SearchAttributeKey.for_keyword("CustomKeyword")
NUM = SearchAttributeKey.for_int("CustomInt")
FLAG = SearchAttributeKey.for_bool("CustomBool")
WHEN = SearchAttributeKey.for_datetime("CustomDatetime")
TAGS = SearchAttributeKey.for_keyword_list("CustomTags")
WHEN_VAL = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)


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


@workflow.defn
class Parent:
    @workflow.run
    async def run(self) -> str:
        base = workflow.info().workflow_id
        child: str = await workflow.execute_child_workflow(
            Completer.run, "kid", id=f"{base}_child", task_queue=TASK_QUEUE
        )
        return child


@workflow.defn
class CanOnce:
    @workflow.run
    async def run(self, hop: bool) -> str:
        if hop:
            workflow.continue_as_new(args=[False])
        return "done"


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[Completer, Failer, Holder, Parent, CanOnce],
        activities=[],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
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


async def test_list_by_workflow_id_returns_run_chain() -> None:
    async with _env() as client:
        # Reuse one workflow id three times: the runs become chain-1, chain-1--r1,
        # chain-1--r2. WorkflowId = X must return the whole chain (as in Temporal,
        # where a WorkflowId query returns every run of that id), not just run 0.
        for v in ["a", "b", "c"]:
            await client.execute_workflow(
                Completer.run, v, id="chain-1", task_queue=TASK_QUEUE
            )

        rows = await _collect(client.list_workflows("WorkflowId = 'chain-1'"))
        assert {r.id for r in rows} == {"chain-1"}  # all share the base workflow id
        chain_runs = {"chain-1", "chain-1--r1", "chain-1--r2"}
        assert {r.run_id for r in rows} == chain_runs  # three distinct runs

        # map_histories() yields each run's WorkflowHistory (what feeds the
        # Replayer): one per run, each snapshotting its own run.
        histories = [
            h
            async for h in client.list_workflows(
                "WorkflowId = 'chain-1'"
            ).map_histories()
        ]
        assert {h.run_id for h in histories} == chain_runs
        assert all(h.workflow_type == "Completer" for h in histories)


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


# --- list: query-less, ordering, empty, negation ------------------------------


async def test_list_no_query_returns_everything() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        await client.execute_workflow(
            Completer.run, "b", id="c2", task_queue=TASK_QUEUE
        )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        rows = await _collect(client.list_workflows())
        assert {r.id for r in rows} == {"c1", "c2", "h1"}

        await holder.signal(Holder.finish)
        await holder.result()


async def test_list_ordering_is_newest_first() -> None:
    async with _env() as client:
        # execute_workflow serializes, so created_at strictly increases c0<c1<c2.
        for i in range(3):
            await client.execute_workflow(
                Completer.run, str(i), id=f"c{i}", task_queue=TASK_QUEUE
            )
        rows = await _collect(client.list_workflows("WorkflowType = 'Completer'"))
        assert [r.id for r in rows] == ["c2", "c1", "c0"]


async def test_list_empty_result() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        assert await _collect(client.list_workflows("WorkflowType = 'Nope'")) == []


async def test_list_workflow_type_negation() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        rows = await _collect(client.list_workflows("WorkflowType != 'Holder'"))
        assert {r.id for r in rows} == {"c1"}

        await holder.signal(Holder.finish)
        await holder.result()


# --- list: terminal-status mappings -------------------------------------------


async def test_list_status_terminated() -> None:
    async with _env() as client:
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)
        await holder.terminate()
        rows = await _collect(client.list_workflows("ExecutionStatus = 'Terminated'"))
        assert {r.id for r in rows} == {"h1"}
        assert rows[0].status == WorkflowExecutionStatus.TERMINATED


async def test_list_canceled_distinct_from_failed() -> None:
    # Both Canceled and Failed are stored as DBOS ERROR; the post-filter must
    # tell them apart by the recorded marker.
    async with _env() as client:
        with pytest.raises(WorkflowFailureError):
            await client.execute_workflow(Failer.run, id="f1", task_queue=TASK_QUEUE)
        holder = await client.start_workflow(Holder.run, id="x1", task_queue=TASK_QUEUE)
        await holder.cancel()
        with pytest.raises(WorkflowFailureError):
            await holder.result()

        canceled = await _collect(client.list_workflows("ExecutionStatus = 'Canceled'"))
        assert {r.id for r in canceled} == {"x1"}
        failed = await _collect(client.list_workflows("ExecutionStatus = 'Failed'"))
        assert {r.id for r in failed} == {"f1"}


# --- list: full field population, parent links, run chains --------------------


async def test_list_populates_all_execution_fields() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run,
            "hi",
            id="c1",
            task_queue=TASK_QUEUE,
            memo={"owner": "ada"},
            search_attributes=TypedSearchAttributes(
                [SearchAttributePair(KW, "vip"), SearchAttributePair(NUM, 7)]
            ),
        )
        rows = await _collect(client.list_workflows("WorkflowId = 'c1'"))
        assert len(rows) == 1
        e = rows[0]
        assert e.id == "c1"
        assert e.run_id == "c1"  # base run: DBOS id == Temporal id
        assert e.workflow_type == "Completer"
        assert e.task_queue == TASK_QUEUE
        assert e.status == WorkflowExecutionStatus.COMPLETED
        assert e.start_time is not None and e.close_time is not None
        assert e.close_time >= e.start_time
        assert e.parent_id is None
        assert e.typed_search_attributes[KW] == "vip"
        assert e.typed_search_attributes[NUM] == 7
        assert e.search_attributes["CustomKeyword"] == ["vip"]  # legacy untyped view
        assert await e.memo() == {"owner": "ada"}


async def test_list_child_workflow_has_parent_id() -> None:
    async with _env() as client:
        assert (
            await client.execute_workflow(Parent.run, id="par1", task_queue=TASK_QUEUE)
            == "kid"
        )
        children = await _collect(client.list_workflows("WorkflowType = 'Completer'"))
        assert len(children) == 1
        assert children[0].id == "par1_child"
        assert children[0].parent_id == "par1"  # cross-chain parent link
        parents = await _collect(client.list_workflows("WorkflowType = 'Parent'"))
        assert parents[0].parent_id is None


async def test_list_continue_as_new_chain_rows() -> None:
    # Each run-chain link is its own row (run_id = DBOS id); same-chain links are
    # continuations, not parents, so parent_id stays None.
    async with _env() as client:
        assert (
            await client.execute_workflow(
                CanOnce.run, True, id="can1", task_queue=TASK_QUEUE
            )
            == "done"
        )
        rows = await _collect(client.list_workflows("WorkflowType = 'CanOnce'"))
        by_run = {r.run_id: r for r in rows}
        assert set(by_run) == {"can1", "can1--r1"}
        assert all(r.id == "can1" for r in rows)
        assert all(r.parent_id is None for r in rows)
        assert by_run["can1"].status == WorkflowExecutionStatus.CONTINUED_AS_NEW
        assert by_run["can1--r1"].status == WorkflowExecutionStatus.COMPLETED


# --- list: pagination correctness under post-filtering ------------------------


async def test_list_pagination_post_filter_no_skip_or_dup() -> None:
    # Interleave completers and failers, then page Failed with a tiny page so
    # most rows in each raw page are dropped. The iterator must advance by the
    # raw rows scanned, not by survivors — otherwise it skips or duplicates.
    async with _env() as client:
        for i in range(3):
            await client.execute_workflow(
                Completer.run, "ok", id=f"c{i}", task_queue=TASK_QUEUE
            )
            with pytest.raises(WorkflowFailureError):
                await client.execute_workflow(
                    Failer.run, id=f"f{i}", task_queue=TASK_QUEUE
                )

        failed = await _collect(
            client.list_workflows("ExecutionStatus = 'Failed'", page_size=2)
        )
        ids = [r.id for r in failed]
        assert sorted(ids) == ["f0", "f1", "f2"]
        assert len(ids) == len(set(ids))  # no duplicates


# --- list: search-attribute value types ---------------------------------------


async def test_list_search_attribute_value_types() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run,
            "x",
            id="typed",
            task_queue=TASK_QUEUE,
            search_attributes=TypedSearchAttributes(
                [
                    SearchAttributePair(NUM, 5),
                    SearchAttributePair(FLAG, True),
                    SearchAttributePair(WHEN, WHEN_VAL),
                    SearchAttributePair(TAGS, ["red", "blue"]),
                ]
            ),
        )
        await client.execute_workflow(
            Completer.run, "y", id="other", task_queue=TASK_QUEUE
        )

        async def ids(query: str) -> set[str]:
            return {r.id for r in await _collect(client.list_workflows(query))}

        assert await ids("CustomInt = 5") == {"typed"}
        assert await ids("CustomBool = true") == {"typed"}
        assert await ids(f"CustomDatetime = '{WHEN_VAL.isoformat()}'") == {"typed"}
        # a non-matching value finds nothing.
        assert await ids("CustomInt = 999") == set()
        # DEVIATION (D15): keyword-LIST attributes are not filterable. The value
        # is stored as a JSON array (["red", "blue"]) and our containment filter
        # is scalar-shaped ({"v": "red"}); with no cluster type registry the
        # query can't know to wrap the value as an array, so it never matches.
        assert await ids("CustomTags = 'red'") == set()


# --- list: time ranges + limit edge ------------------------------------------


async def test_list_time_range_filter() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "x", id="c1", task_queue=TASK_QUEUE
        )
        past = "2000-01-01T00:00:00+00:00"
        future = "2100-01-01T00:00:00+00:00"

        assert {
            r.id
            for r in await _collect(client.list_workflows(f"StartTime >= '{past}'"))
        } == {"c1"}
        assert await _collect(client.list_workflows(f"StartTime >= '{future}'")) == []
        # CloseTime bound on the now-completed workflow.
        assert {
            r.id
            for r in await _collect(client.list_workflows(f"CloseTime >= '{past}'"))
        } == {"c1"}


async def test_list_limit_exceeds_available() -> None:
    async with _env() as client:
        for i in range(3):
            await client.execute_workflow(
                Completer.run, str(i), id=f"c{i}", task_queue=TASK_QUEUE
            )
        rows = await _collect(
            client.list_workflows("WorkflowType = 'Completer'", limit=100)
        )
        assert len(rows) == 3


# --- count: query-less, clean filters, zero, grouped-with-filter --------------


async def test_count_no_query_and_clean_filters() -> None:
    async with _env() as client:
        for i in range(3):
            await client.execute_workflow(
                Completer.run, "x", id=f"order-{i}", task_queue=TASK_QUEUE
            )
        past = "2000-01-01T00:00:00+00:00"

        assert (await client.count_workflows()).count == 3
        # STARTS_WITH exercises the prefix->list wrapping for the aggregate.
        assert (
            await client.count_workflows("WorkflowId STARTS_WITH 'order-'")
        ).count == 3
        assert (
            await client.count_workflows(
                "WorkflowType = 'Completer' AND ExecutionStatus = 'Completed'"
            )
        ).count == 3
        assert (await client.count_workflows(f"StartTime >= '{past}'")).count == 3


async def test_count_terminated_and_zero() -> None:
    async with _env() as client:
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)
        await holder.terminate()
        assert (await holder.describe()).status == WorkflowExecutionStatus.TERMINATED
        # Terminated maps to DBOS CANCELLED (clean) — aggregate-countable.
        assert (
            await client.count_workflows("ExecutionStatus = 'Terminated'")
        ).count == 1
        assert (await client.count_workflows("WorkflowType = 'Nope'")).count == 0


async def test_count_group_by_workflow_type_with_filter() -> None:
    async with _env() as client:
        await client.execute_workflow(
            Completer.run, "a", id="c1", task_queue=TASK_QUEUE
        )
        await client.execute_workflow(
            Completer.run, "b", id="c2", task_queue=TASK_QUEUE
        )
        holder = await client.start_workflow(Holder.run, id="h1", task_queue=TASK_QUEUE)

        # Filter to Completed first, then group — the running Holder drops out.
        count = await client.count_workflows(
            "ExecutionStatus = 'Completed' GROUP BY WorkflowType"
        )
        assert _groups(count) == {"Completer": 2}
        assert count.count == 2

        await holder.signal(Holder.finish)
        await holder.result()


# --- internal plumbing is invisible -------------------------------------------


async def test_visibility_excludes_internal_plumbing_workflows() -> None:
    # A schedule fire runs an internal `__temporal_schedule_fire` DBOS workflow
    # that in turn starts the user action workflow. list/count must surface only
    # the latter (a `wf:` workflow), never the dispatcher.
    async with _env() as client:
        sched = await client.create_schedule(
            "sched-x",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    Completer.run, "sched", id="sched-action", task_queue=TASK_QUEUE
                ),
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=timedelta(minutes=10))]
                ),
            ),
        )
        await sched.trigger()

        async def _action_started() -> None:
            rows = await client._dbos_client.list_workflows_async(name="wf:Completer")
            assert rows, "scheduled action has not started yet"

        await retry_until_success_async(_action_started)
        await sched.delete()

        # The internal dispatcher really is present in the raw DBOS view...
        fire_rows = await client._dbos_client.list_workflows_async(
            name="__temporal_schedule_fire"
        )
        assert fire_rows, "expected a __temporal_schedule_fire row to exist"
        fire_ids = {r.workflow_id for r in fire_rows}

        # ...but the visibility API shows only the user (wf:) workflow.
        rows = await _collect(client.list_workflows())
        assert {r.workflow_type for r in rows} == {"Completer"}
        assert fire_ids.isdisjoint({r.run_id for r in rows})
        assert (await client.count_workflows()).count == 1
        assert _groups(await client.count_workflows("GROUP BY WorkflowType")) == {
            "Completer": 1
        }
