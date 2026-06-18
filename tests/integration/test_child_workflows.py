"""Child workflows (§6.6), in-process: parent/child results, default ids,
failure cause fidelity, signaling children, and ParentClosePolicy. The
SIGKILL re-attach proof lives in test_child_workflows_recovery.py.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import (
    ApplicationError,
    CancelledError,
    ChildWorkflowError,
    TerminatedError,
    WorkflowAlreadyStartedError,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "phase2-child-tq"


@activity.defn
async def record(path: str, label: str) -> str:
    with open(path, "a") as f:
        f.write(label + "\n")
    return label


@workflow.defn
class ComposeChild:
    @workflow.run
    async def run(self, greeting: str, name: str) -> str:
        await workflow.sleep(0.05)
        return f"{greeting}, {name}!"


@workflow.defn
class FailingChild:
    @workflow.run
    async def run(self) -> None:
        raise ApplicationError("child exploded", type="ChildBoom")


@workflow.defn
class WaitingChild:
    def __init__(self) -> None:
        self.go = False

    @workflow.signal
    def release(self) -> None:
        self.go = True

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.go)
        return "released"


@workflow.defn
class SlowRecordingChild:
    @workflow.run
    async def run(self, path: str) -> str:
        await workflow.sleep(3.0)
        result: str = await workflow.execute_activity(
            record,
            args=[path, "child-done"],
            start_to_close_timeout=timedelta(seconds=10),
        )
        return result


@workflow.defn
class ParentWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_child_workflow(
            ComposeChild.run, args=["Hello", name]
        )
        return result


@workflow.defn
class CatchingParent:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        try:
            await workflow.execute_child_workflow(FailingChild.run, id="failing-child")
            return {"unreachable": True}
        except ChildWorkflowError as err:
            cause = err.cause
            assert isinstance(cause, ApplicationError)
            return {
                "workflow_type": err.workflow_type,
                "workflow_id": err.workflow_id,
                "cause_type": cause.type,
                "cause_message": cause.message,
            }


@workflow.defn
class SignalingParent:
    @workflow.run
    async def run(self) -> str:
        handle = await workflow.start_child_workflow(
            WaitingChild.run, id="waiting-child"
        )
        await handle.signal(WaitingChild.release)
        result: str = await handle
        return f"child said: {result}"


@workflow.defn
class HandleCancelParent:
    """Exercises the ChildWorkflowHandle Task-style API: cancel one child via
    ``handle.cancel()`` and read a sibling's value via the synchronous
    ``handle.result()`` accessor. The doomed child uses ABANDON parent-close so
    it outlives the parent and finishes its own cooperative cancellation
    (→ CANCELED), letting the test observe that cancel() actually reached it."""

    @workflow.run
    async def run(self) -> str:
        doomed = await workflow.start_child_workflow(
            WaitingChild.run,
            id="handle-cancel-doomed",
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )
        winner = await workflow.start_child_workflow(
            WaitingChild.run, id="handle-result-winner"
        )
        doomed.cancel()
        await winner.signal(WaitingChild.release)
        await winner
        result: str = winner.result()
        return result


@workflow.defn
class ClosingParent:
    """Starts a long-running child and returns immediately; the child's fate
    is decided by ParentClosePolicy."""

    @workflow.run
    async def run(self, path: str, policy_name: str) -> str:
        policy = workflow.ParentClosePolicy[policy_name]
        await workflow.start_child_workflow(
            SlowRecordingChild.run,
            path,
            id=f"closing-child-{policy_name.lower()}",
            parent_close_policy=policy,
        )
        return "parent done"


@workflow.defn
class ParkingParent:
    """Starts one TERMINATE-policy child and one ABANDON-policy child, then
    parks forever — the target for terminate-applies-parent-close tests."""

    @workflow.run
    async def run(self, terminate_path: str, abandon_path: str) -> str:
        await workflow.start_child_workflow(
            SlowRecordingChild.run,
            terminate_path,
            id="parked-child-terminate",
            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
        )
        await workflow.start_child_workflow(
            SlowRecordingChild.run,
            abandon_path,
            id="parked-child-abandon",
            parent_close_policy=workflow.ParentClosePolicy.ABANDON,
        )
        await workflow.wait_condition(lambda: False)
        return "unreachable"


@workflow.defn
class DuplicateIdParent:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_child_workflow(
                ComposeChild.run, args=["Hi", "X"], id="taken-id"
            )
            return "no-error"
        except WorkflowAlreadyStartedError as err:
            return f"already-started:{err.workflow_id}"


@workflow.defn
class ExternalToucher:
    @workflow.run
    async def run(self, target: str, action: str) -> str:
        handle = workflow.get_external_workflow_handle(target)
        if action == "signal":
            await handle.signal("release")
        else:
            await handle.cancel(reason="external cancel")
        return "sent"


# Fast backoff so a multi-attempt child chain completes well under a second.
FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(milliseconds=20), maximum_attempts=5
)


@workflow.defn
class RetryingChild:
    """Fails until the chain reaches attempt 3, then succeeds — proving the
    child honors its own retry_policy AND the parent follows the retry chain."""

    @workflow.run
    async def run(self) -> int:
        attempt = workflow.info().attempt
        if attempt < 3:
            raise ApplicationError(f"child failing attempt {attempt}")
        return attempt


@workflow.defn
class RetryParent:
    @workflow.run
    async def run(self) -> int:
        winning_attempt: int = await workflow.execute_child_workflow(
            RetryingChild.run, id="retrying-child", retry_policy=FAST_RETRY
        )
        return winning_attempt


@workflow.defn
class TimingOutChild:
    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(30)
        return "should-not-finish"


@workflow.defn
class TimeoutParent:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_child_workflow(
                TimingOutChild.run,
                id="timing-out-child",
                run_timeout=timedelta(seconds=1),
            )
            return "no-timeout"
        except ChildWorkflowError as err:
            # run_timeout natively cancels the child → TERMINATED in our scheme
            # (D19), surfaced to the parent as a terminated child.
            return f"timed-out:{type(err.cause).__name__}"


@workflow.defn
class AlwaysFailingChild:
    @workflow.run
    async def run(self) -> None:
        raise ApplicationError(
            f"always fails attempt {workflow.info().attempt}", type="ChildAlwaysFails"
        )


@workflow.defn
class RetryExhaustParent:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        try:
            await workflow.execute_child_workflow(
                AlwaysFailingChild.run,
                id="exhausting-child",
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=20), maximum_attempts=2
                ),
            )
            return {"unreachable": True}
        except ChildWorkflowError as err:
            cause = err.cause
            assert isinstance(cause, ApplicationError)
            # The parent must see the LAST attempt's failure (the follow loop
            # ran out the retry chain), not the first.
            return {"cause_type": cause.type, "cause_message": cause.message}


ALL_WORKFLOWS = [
    ComposeChild,
    FailingChild,
    WaitingChild,
    SlowRecordingChild,
    RetryingChild,
    RetryParent,
    TimingOutChild,
    TimeoutParent,
    AlwaysFailingChild,
    RetryExhaustParent,
    ParentWorkflow,
    CatchingParent,
    SignalingParent,
    HandleCancelParent,
    ClosingParent,
    ParkingParent,
    DuplicateIdParent,
    ExternalToucher,
]


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=ALL_WORKFLOWS,
        activities=[record],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def test_parent_child_result_and_default_id() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            ParentWorkflow.run, "World", id="parent-basic", task_queue=TASK_QUEUE
        )
        assert result == "Hello, World!"
        # Default child id: {parent_dbos_id}_{seq} (README deviation #5).
        child = client.get_workflow_handle("parent-basic_1")
        description = await child.describe()
        assert description.status == WorkflowExecutionStatus.COMPLETED
        assert description.workflow_type == "ComposeChild"
        assert description.task_queue == TASK_QUEUE  # inherited parent queue


async def test_child_failure_cause_fidelity() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            CatchingParent.run, id="parent-catching", task_queue=TASK_QUEUE
        )
        assert result == {
            "workflow_type": "FailingChild",
            "workflow_id": "failing-child",
            "cause_type": "ChildBoom",
            "cause_message": "child exploded",
        }


async def test_parent_signals_child() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            SignalingParent.run, id="parent-signaling", task_queue=TASK_QUEUE
        )
        assert result == "child said: released"


async def test_child_retry_policy_honored() -> None:
    # The child fails twice and succeeds on attempt 3. The parent receives the
    # winning attempt only if (a) the child's retry_policy drove the retries and
    # (b) the child-result step followed the retry chain to the final run.
    async with _env() as client:
        result = await client.execute_workflow(
            RetryParent.run, id="parent-child-retry", task_queue=TASK_QUEUE
        )
        assert result == 3
        # The chain advanced past run 0 (retries are new runs in the chain).
        current = await client._current_run("retrying-child")
        assert current is not None and current[0] >= 2


async def test_child_run_timeout_honored() -> None:
    # The child would sleep 30s; a 1s run_timeout fires first and terminates it.
    # If run_timeout were silently dropped, the parent would hang past the test
    # timeout instead of catching a ChildWorkflowError within ~1s.
    async with _env() as client:
        result = await client.execute_workflow(
            TimeoutParent.run, id="parent-child-timeout", task_queue=TASK_QUEUE
        )
        assert result == f"timed-out:{TerminatedError.__name__}"


async def test_child_retry_exhaustion_surfaces_last_failure() -> None:
    # The child always fails; with maximum_attempts=2 the chain runs attempts 1
    # and 2, then the terminal failure surfaces. The parent must see attempt 2's
    # failure (the child-result step followed the chain to the end), not the
    # first attempt's.
    async with _env() as client:
        result = await client.execute_workflow(
            RetryExhaustParent.run, id="parent-child-exhaust", task_queue=TASK_QUEUE
        )
        assert result == {
            "cause_type": "ChildAlwaysFails",
            "cause_message": "always fails attempt 2",
        }
        # Exactly two runs in the chain: run 0 (attempt 1) and run 1 (attempt 2).
        current = await client._current_run("exhausting-child")
        assert current is not None and current[0] == 1


async def _wait_for_status(
    client: Client, workflow_id: str, expected: WorkflowExecutionStatus
) -> Optional[WorkflowExecutionStatus]:
    handle = client.get_workflow_handle(workflow_id)
    deadline = time.monotonic() + 20
    status = None
    while time.monotonic() < deadline:
        try:
            status = (await handle.describe()).status
        except RuntimeError:
            status = None  # workflow row doesn't exist yet
        if status == expected:
            return status
        await asyncio.sleep(0.2)
    return status


async def test_child_handle_cancel_and_result() -> None:
    """ChildWorkflowHandle.cancel() drives a child to CANCELED; .result()
    returns a completed sibling's value synchronously (Task-style handle)."""
    async with _env() as client:
        result = await client.execute_workflow(
            HandleCancelParent.run, id="handle-api-parent", task_queue=TASK_QUEUE
        )
        assert result == "released"
        status = await _wait_for_status(
            client, "handle-cancel-doomed", WorkflowExecutionStatus.CANCELED
        )
        assert status == WorkflowExecutionStatus.CANCELED


async def test_parent_close_terminate(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    async with _env() as client:
        result = await client.execute_workflow(
            ClosingParent.run,
            args=[str(effects), "TERMINATE"],
            id="parent-terminate",
            task_queue=TASK_QUEUE,
        )
        assert result == "parent done"
        status = await _wait_for_status(
            client, "closing-child-terminate", WorkflowExecutionStatus.TERMINATED
        )
        assert status == WorkflowExecutionStatus.TERMINATED
    # The terminated child never reached its recording activity.
    assert not effects.exists()


async def test_terminate_applies_parent_close_policies(tmp_path: Path) -> None:
    """Terminating a parent runs no workflow code, so the recorded
    ParentClosePolicy must be applied client-side: TERMINATE children die,
    ABANDON children survive."""
    terminate_effects = tmp_path / "terminate-effects"
    abandon_effects = tmp_path / "abandon-effects"
    async with _env() as client:
        handle = await client.start_workflow(
            ParkingParent.run,
            args=[str(terminate_effects), str(abandon_effects)],
            id="parking-parent",
            task_queue=TASK_QUEUE,
        )
        # Wait until both children are durably started and running.
        assert (
            await _wait_for_status(
                client, "parked-child-abandon", WorkflowExecutionStatus.RUNNING
            )
            == WorkflowExecutionStatus.RUNNING
        )
        await handle.terminate()

        assert (await handle.describe()).status == WorkflowExecutionStatus.TERMINATED
        status = await _wait_for_status(
            client, "parked-child-terminate", WorkflowExecutionStatus.TERMINATED
        )
        assert status == WorkflowExecutionStatus.TERMINATED
        status = await _wait_for_status(
            client, "parked-child-abandon", WorkflowExecutionStatus.COMPLETED
        )
        assert status == WorkflowExecutionStatus.COMPLETED
    assert not terminate_effects.exists()
    assert abandon_effects.read_text() == "child-done\n"


async def test_parent_close_abandon(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    async with _env() as client:
        result = await client.execute_workflow(
            ClosingParent.run,
            args=[str(effects), "ABANDON"],
            id="parent-abandon",
            task_queue=TASK_QUEUE,
        )
        assert result == "parent done"
        # The abandoned child keeps running and completes after the parent.
        status = await _wait_for_status(
            client, "closing-child-abandon", WorkflowExecutionStatus.COMPLETED
        )
        assert status == WorkflowExecutionStatus.COMPLETED
    assert effects.read_text() == "child-done\n"


async def test_duplicate_child_id_raises_into_parent() -> None:
    """A child id already in use raises WorkflowAlreadyStartedError into the
    parent (instead of SetWorkflowID silently attaching to the foreign
    workflow)."""
    async with _env() as client:
        taken = await client.start_workflow(
            WaitingChild.run, id="taken-id", task_queue=TASK_QUEUE
        )
        result = await client.execute_workflow(
            DuplicateIdParent.run, id="dup-parent", task_queue=TASK_QUEUE
        )
        assert result == "already-started:taken-id"
        await taken.terminate()


async def test_external_handle_signal() -> None:
    async with _env() as client:
        target = await client.start_workflow(
            WaitingChild.run, id="ext-sig-target", task_queue=TASK_QUEUE
        )
        sent = await client.execute_workflow(
            ExternalToucher.run,
            args=["ext-sig-target", "signal"],
            id="ext-signaler",
            task_queue=TASK_QUEUE,
        )
        assert sent == "sent"
        assert await target.result() == "released"


async def test_external_handle_cancel() -> None:
    async with _env() as client:
        target = await client.start_workflow(
            WaitingChild.run, id="ext-cancel-target", task_queue=TASK_QUEUE
        )
        await client.execute_workflow(
            ExternalToucher.run,
            args=["ext-cancel-target", "cancel"],
            id="ext-canceller",
            task_queue=TASK_QUEUE,
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await target.result()
        assert isinstance(exc_info.value.cause, CancelledError)
        assert (await target.describe()).status == WorkflowExecutionStatus.CANCELED
