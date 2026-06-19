"""In-process behavior tests for ``workflow.patched`` / ``deprecate_patch``
(DESIGN §6.8). These cover the first-execution semantics, per-id memoization,
the durable marker, and the read-only rejection. The replay semantics — old
in-flight runs taking the OLD path after a redeploy, and new runs replaying the
NEW path across a crash — need real recovery and live in
``test_patched_recovery.py``.
"""

from typing import List

import pytest
from dbos import DBOS

from dbosify import workflow
from dbosify._internal import dispatcher
from dbosify._internal.interpreter import PATCH_STEP_NAME


def _marker_ids(workflow_id: str) -> List[str]:
    """The patch ids durably recorded for a run, in checkpoint order."""
    return [
        str(step["output"])
        for step in DBOS.list_workflow_steps(workflow_id)
        if step["function_name"] == PATCH_STEP_NAME
    ]


@workflow.defn
class BranchWorkflow:
    @workflow.run
    async def run(self) -> str:
        if workflow.patched("v2"):
            return "new"
        return "old"


@workflow.defn
class DoublePatchWorkflow:
    @workflow.run
    async def run(self) -> List[bool]:
        # Same id twice: the decision is memoized and the marker written once.
        return [workflow.patched("v2"), workflow.patched("v2")]


@workflow.defn
class MultiPatchWorkflow:
    @workflow.run
    async def run(self) -> List[bool]:
        return [workflow.patched("a"), workflow.patched("b")]


@workflow.defn
class DeprecateWorkflow:
    @workflow.run
    async def run(self) -> str:
        workflow.deprecate_patch("v2")
        return "ok"


@workflow.defn
class QueryPatchWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def is_patched(self) -> bool:
        # Illegal: patched() mutates durable state (records a marker), so it
        # cannot run in the read-only query context (mirrors temporalio, which
        # raises ReadOnlyContextError).
        return workflow.patched("v2")

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.done)
        return "done"


@pytest.mark.usefixtures("dbosify")
def test_first_execution_takes_new_path_and_records_marker() -> None:
    dispatcher.register_worker(workflows=[BranchWorkflow])
    handle = dispatcher.start_workflow(BranchWorkflow, [], workflow_id="patch-branch")
    assert dispatcher.workflow_result(handle) == "new"
    # The newer path ran, so exactly one marker for "v2" is durable.
    assert _marker_ids("patch-branch") == ["v2"]


@pytest.mark.usefixtures("dbosify")
def test_same_id_memoized_writes_one_marker() -> None:
    dispatcher.register_worker(workflows=[DoublePatchWorkflow])
    handle = dispatcher.start_workflow(
        DoublePatchWorkflow, [], workflow_id="patch-double"
    )
    assert dispatcher.workflow_result(handle) == [True, True]
    assert _marker_ids("patch-double") == ["v2"]


@pytest.mark.usefixtures("dbosify")
def test_distinct_ids_each_record_a_marker() -> None:
    dispatcher.register_worker(workflows=[MultiPatchWorkflow])
    handle = dispatcher.start_workflow(
        MultiPatchWorkflow, [], workflow_id="patch-multi"
    )
    assert dispatcher.workflow_result(handle) == [True, True]
    assert _marker_ids("patch-multi") == ["a", "b"]


@pytest.mark.usefixtures("dbosify")
def test_deprecate_patch_returns_none_and_records_marker() -> None:
    dispatcher.register_worker(workflows=[DeprecateWorkflow])
    handle = dispatcher.start_workflow(
        DeprecateWorkflow, [], workflow_id="patch-deprecate"
    )
    assert dispatcher.workflow_result(handle) == "ok"
    # deprecate_patch follows the same use-patch logic: on a fresh run it records
    # the marker so concurrent old runs can still find their position.
    assert _marker_ids("patch-deprecate") == ["v2"]


@pytest.mark.usefixtures("dbosify")
def test_patched_rejected_in_query() -> None:
    dispatcher.register_worker(workflows=[QueryPatchWorkflow])
    handle = dispatcher.start_workflow(
        QueryPatchWorkflow, [], workflow_id="patch-query"
    )
    # A query cannot record a patch marker; the call is rejected.
    with pytest.raises(dispatcher.WorkflowUpdateFailedError):
        dispatcher.query_workflow("patch-query", "is_patched")

    # The rejected query left no marker and did not fail the workflow.
    assert _marker_ids("patch-query") == []
    dispatcher.signal_workflow("patch-query", "finish")
    assert dispatcher.workflow_result(handle) == "done"
