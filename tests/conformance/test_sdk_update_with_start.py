"""Conformance: update-with-start tests adapted from temporalio's
``tests/worker/test_update_with_start.py``.

Driven through this directory's ``client`` fixture + ``new_worker`` adapter
(temporalio's fixtures map onto our Worker/DBOSClient). The server-only and
time-skipping variants are left in the SDK suite; these cover the operation's
validation, single-use enforcement, the no/one/two-param + dataclass overloads,
and the FAIL-conflict / first-execution-run-id path.
"""

import uuid
from dataclasses import dataclass

import pytest

from dbosify import workflow
from dbosify.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowUpdateFailedError,
    WorkflowUpdateStage,
)
from dbosify.common import WorkflowIDConflictPolicy
from dbosify.exceptions import ApplicationError, WorkflowAlreadyStartedError

from .sdk_harness import new_worker

# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------


@dataclass
class DataClass1:
    a: str
    b: str


@dataclass
class DataClass2:
    a: str
    b: str


@dataclass
class WorkflowResult:
    result: str


@dataclass
class UpdateResult:
    result: str


@workflow.defn
class WorkflowForUWS:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self, i: int) -> str:
        await workflow.wait_condition(lambda: self.done)
        return f"workflow-result-{i}"

    @workflow.update
    def my_update(self, s: str) -> str:
        if s == "fail-after-acceptance":
            raise ApplicationError("Workflow deliberate failed update")
        return f"update-result-{s}"


@workflow.defn
class WorkflowCanReturnDataClass:
    def __init__(self) -> None:
        self.received_update = False

    @workflow.run
    async def run(self, arg: str) -> DataClass1:
        await workflow.wait_condition(lambda: self.received_update)
        return DataClass1(a=arg, b="workflow-result")

    @workflow.update
    async def my_update(self, arg: str) -> DataClass2:
        self.received_update = True
        return DataClass2(a=arg, b="update-result")


@workflow.defn
class NoParamWorkflow:
    def __init__(self) -> None:
        self.received_update = False

    @workflow.run
    async def my_workflow_run(self) -> WorkflowResult:
        await workflow.wait_condition(lambda: self.received_update)
        return WorkflowResult(result="workflow-result")

    @workflow.update(name="my_update")
    async def update(self) -> UpdateResult:
        self.received_update = True
        return UpdateResult(result="update-result")


@workflow.defn
class OneParamWorkflow:
    def __init__(self) -> None:
        self.received_update = False

    @workflow.run
    async def my_workflow_run(self, arg: str) -> WorkflowResult:
        await workflow.wait_condition(lambda: self.received_update)
        return WorkflowResult(result=arg)

    @workflow.update(name="my_update")
    async def update(self, arg: str) -> UpdateResult:
        self.received_update = True
        return UpdateResult(result=arg)


@workflow.defn
class TwoParamWorkflow:
    def __init__(self) -> None:
        self.received_update = False

    @workflow.run
    async def my_workflow_run(self, arg1: str, arg2: str) -> WorkflowResult:
        await workflow.wait_condition(lambda: self.received_update)
        return WorkflowResult(result=arg1 + "-" + arg2)

    @workflow.update(name="my_update")
    async def update(self, arg1: str, arg2: str) -> UpdateResult:
        self.received_update = True
        return UpdateResult(result=arg1 + "-" + arg2)


def _wf(prefix: str = "uws") -> str:
    return f"{prefix}-{uuid.uuid4()}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_with_start_workflow_operation_requires_conflict_policy() -> None:
    # Explicit UNSPECIFIED is rejected (ValueError) ...
    with pytest.raises(ValueError):
        WithStartWorkflowOperation(
            WorkflowForUWS.run,
            0,
            id="wid-1",
            id_conflict_policy=WorkflowIDConflictPolicy.UNSPECIFIED,
            task_queue="test-queue",
        )
    # ... and omitting it entirely is a TypeError (required keyword).
    with pytest.raises(TypeError):
        WithStartWorkflowOperation(  # type: ignore[call-arg]
            WorkflowForUWS.run,
            0,
            id="wid-1",
            task_queue="test-queue",
        )


async def test_with_start_workflow_operation_cannot_be_reused(client: Client) -> None:
    async with new_worker(client, WorkflowForUWS) as worker:
        start_op = WithStartWorkflowOperation(
            WorkflowForUWS.run,
            0,
            id=_wf(),
            task_queue=worker.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )

        async def go(op: WithStartWorkflowOperation) -> None:
            await client.start_update_with_start_workflow(
                WorkflowForUWS.my_update,
                "1",
                wait_for_stage=WorkflowUpdateStage.COMPLETED,
                start_workflow_operation=op,
            )

        await go(start_op)
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await go(start_op)


async def test_workflow_and_update_can_return_dataclass(client: Client) -> None:
    async with new_worker(client, WorkflowCanReturnDataClass) as worker:

        def make_op(workflow_id: str) -> WithStartWorkflowOperation:
            return WithStartWorkflowOperation(
                WorkflowCanReturnDataClass.run,
                "workflow-arg",
                id=workflow_id,
                task_queue=worker.task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            )

        # Typed (method-reference) overload.
        op = make_op(_wf())
        uh = await client.start_update_with_start_workflow(
            WorkflowCanReturnDataClass.my_update,
            "update-arg",
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op,
        )
        assert await uh.result() == DataClass2(a="update-arg", b="update-result")
        wf = await op.workflow_handle()
        assert await wf.result() == DataClass1(a="workflow-arg", b="workflow-result")

        # String-name overload (result_type supplied explicitly).
        op = WithStartWorkflowOperation(
            "WorkflowCanReturnDataClass",
            "workflow-arg",
            id=_wf(),
            task_queue=worker.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            result_type=DataClass1,
        )
        uh = await client.start_update_with_start_workflow(
            "my_update",
            "update-arg",
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op,
            result_type=DataClass2,
        )
        assert await uh.result() == DataClass2(a="update-arg", b="update-result")
        wf = await op.workflow_handle()
        assert await wf.result() == DataClass1(a="workflow-arg", b="workflow-result")


async def test_update_with_start_no_param(client: Client) -> None:
    async with new_worker(client, NoParamWorkflow) as worker:
        op = WithStartWorkflowOperation(
            NoParamWorkflow.my_workflow_run,
            id=_wf(),
            task_queue=worker.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
        uh = await client.start_update_with_start_workflow(
            NoParamWorkflow.update,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op,
        )
        assert await uh.result() == UpdateResult(result="update-result")
        wf = await op.workflow_handle()
        assert await wf.result() == WorkflowResult(result="workflow-result")


async def test_update_with_start_one_param(client: Client) -> None:
    async with new_worker(client, OneParamWorkflow) as worker:
        op = WithStartWorkflowOperation(
            OneParamWorkflow.my_workflow_run,
            "workflow-arg",
            id=_wf(),
            task_queue=worker.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
        uh = await client.start_update_with_start_workflow(
            OneParamWorkflow.update,
            "update-arg",
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op,
        )
        assert await uh.result() == UpdateResult(result="update-arg")
        wf = await op.workflow_handle()
        assert await wf.result() == WorkflowResult(result="workflow-arg")


async def test_update_with_start_two_param(client: Client) -> None:
    async with new_worker(client, TwoParamWorkflow) as worker:
        op = WithStartWorkflowOperation(
            TwoParamWorkflow.my_workflow_run,
            args=("workflow-arg1", "workflow-arg2"),
            id=_wf(),
            task_queue=worker.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
        uh = await client.start_update_with_start_workflow(
            TwoParamWorkflow.update,
            args=("update-arg1", "update-arg2"),
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op,
        )
        assert await uh.result() == UpdateResult(result="update-arg1-update-arg2")
        wf = await op.workflow_handle()
        assert await wf.result() == WorkflowResult(
            result="workflow-arg1-workflow-arg2"
        )


async def test_update_with_start_sets_first_execution_run_id(client: Client) -> None:
    async with new_worker(client, WorkflowForUWS) as worker:

        def make_op(workflow_id: str) -> WithStartWorkflowOperation:
            return WithStartWorkflowOperation(
                WorkflowForUWS.run,
                0,
                id=workflow_id,
                task_queue=worker.task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
            )

        wid_1 = _wf()
        # First UWS succeeds and sets the first-execution run id.
        op1 = make_op(wid_1)
        uh1 = await client.start_update_with_start_workflow(
            WorkflowForUWS.my_update,
            "1",
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op1,
        )
        assert (await op1.workflow_handle()).first_execution_run_id is not None
        assert await uh1.result() == "update-result-1"

        # Second UWS (same id, FAIL) is rejected on both the update and the handle.
        op2 = make_op(wid_1)
        with pytest.raises(WorkflowAlreadyStartedError):
            await client.start_update_with_start_workflow(
                WorkflowForUWS.my_update,
                "2",
                wait_for_stage=WorkflowUpdateStage.COMPLETED,
                start_workflow_operation=op2,
            )
        with pytest.raises(WorkflowAlreadyStartedError):
            await op2.workflow_handle()

        # Third UWS starts a fresh run, but the update fails after acceptance.
        op3 = make_op(_wf())
        uh3 = await client.start_update_with_start_workflow(
            WorkflowForUWS.my_update,
            "fail-after-acceptance",
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            start_workflow_operation=op3,
        )
        assert (await op3.workflow_handle()).first_execution_run_id is not None
        with pytest.raises(WorkflowUpdateFailedError):
            await uh3.result()
