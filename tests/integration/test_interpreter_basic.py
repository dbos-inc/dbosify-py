"""In-process interpreter tests: happy paths plus the §4.3 cases that don't
need a SIGKILL (3: gather + retry exhaustion; 4: update validator,
accepted half; 6: non-failure exception keeps the workflow running until the
implementation is swapped). The SIGKILL-recovery suite lives in
test_interpreter_recovery.py.
"""

import asyncio
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pytest
from dbos import DBOS

from temporal_dbos import activity, workflow
from temporal_dbos._internal import dispatcher
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import ActivityError, ApplicationError, RetryState

# Per-test mutable state activities reach into (reset by fixtures/tests).
attempt_counts: Dict[str, int] = {}


@activity.defn
async def compose(name: str, n: int) -> str:
    return f"hello-{name}-{n}"


@activity.defn
async def slow_compose(name: str, n: int) -> str:
    await asyncio.sleep(0.5)
    return f"hello-{name}-{n}"


@activity.defn
async def always_fails(key: str) -> str:
    attempt_counts[key] = attempt_counts.get(key, 0) + 1
    raise ValueError(f"boom {attempt_counts[key]}")


@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        first = await workflow.execute_activity(
            compose, args=[name, 1], start_to_close_timeout=timedelta(seconds=5)
        )
        await workflow.sleep(0.05)
        second = await workflow.execute_activity(
            compose, args=[name, 2], start_to_close_timeout=timedelta(seconds=5)
        )
        return f"{first}|{second}"


@workflow.defn
class ApprovalWorkflow:
    def __init__(self) -> None:
        self.approver: Optional[str] = None

    @workflow.signal
    def approve(self, who: str) -> None:
        self.approver = who

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.approver is not None)
        return f"approved by {self.approver}"


@workflow.defn
class CounterWorkflow:
    def __init__(self) -> None:
        self.total = 0
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def current(self) -> int:
        return self.total

    @workflow.update
    def add(self, n: int) -> int:
        self.total += n
        return self.total

    @add.validator
    def _validate_add(self, n: int) -> None:
        if n < 0:
            raise ApplicationError("negative amounts not allowed", type="BadAmount")

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self.done)
        return self.total


@workflow.defn
class GatherWorkflow:
    @workflow.run
    async def run(self, key: str) -> Dict[str, Any]:
        results = await asyncio.gather(
            workflow.execute_activity(
                compose, args=["g", 1], start_to_close_timeout=timedelta(seconds=5)
            ),
            workflow.execute_activity(
                always_fails,
                key,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=10), maximum_attempts=3
                ),
            ),
            workflow.execute_activity(
                compose, args=["g", 2], start_to_close_timeout=timedelta(seconds=5)
            ),
            return_exceptions=True,
        )
        err = results[1]
        assert isinstance(err, ActivityError)
        cause = err.cause
        assert isinstance(cause, ApplicationError)
        return {
            "ok_results": [results[0], results[2]],
            "error_type": type(err).__name__,
            "activity_type": err.activity_type,
            "retry_state": (
                err.retry_state.name if err.retry_state is not None else None
            ),
            "cause_type": cause.type,
            "cause_message": cause.message,
        }


@workflow.defn
class WaitRaceWorkflow:
    @workflow.run
    async def run(self) -> Dict[str, Any]:
        slow = asyncio.create_task(
            workflow.execute_activity(
                slow_compose,
                args=["slow", 1],
                start_to_close_timeout=timedelta(seconds=5),
            )
        )
        timer = asyncio.create_task(workflow.sleep(0.05))
        done, pending = await workflow.wait(
            [slow, timer], return_when=asyncio.FIRST_COMPLETED
        )
        first_done = "timer" if timer in done else "slow"
        # Collect the rest in completion order via as_completed.
        ordered = [await coro for coro in workflow.as_completed([slow])]
        return {
            "first_done": first_done,
            "done_type": type(done).__name__,
            "pending_count": len(pending),
            "slow_result": ordered[0],
        }


@workflow.defn(name="DeterminismProbe")
class DeterminismProbe:
    @workflow.run
    async def run(self) -> List[str]:
        return [
            str(workflow.uuid4()),
            str(workflow.random().random()),
            workflow.now().isoformat(),
        ]


@pytest.mark.usefixtures("tdb")
def test_activities_and_sleep() -> None:
    dispatcher.register_worker(workflows=[GreetingWorkflow], activities=[compose])
    handle = dispatcher.start_workflow(GreetingWorkflow, ["world"], workflow_id="greet")
    assert dispatcher.workflow_result(handle) == "hello-world-1|hello-world-2"


@pytest.mark.usefixtures("tdb")
def test_signal_and_wait_condition() -> None:
    dispatcher.register_worker(workflows=[ApprovalWorkflow])
    handle = dispatcher.start_workflow(ApprovalWorkflow, [], workflow_id="approval")
    dispatcher.signal_workflow("approval", "approve", ["alice"])
    assert dispatcher.workflow_result(handle) == "approved by alice"


@pytest.mark.usefixtures("tdb")
def test_updates_queries_and_validator() -> None:
    dispatcher.register_worker(workflows=[CounterWorkflow])
    handle = dispatcher.start_workflow(CounterWorkflow, [], workflow_id="counter")

    assert dispatcher.execute_update("counter", "add", [5]) == 5
    assert dispatcher.execute_update("counter", "add", [3]) == 8

    with pytest.raises(dispatcher.WorkflowUpdateFailedError) as exc_info:
        dispatcher.execute_update("counter", "add", [-1])
    cause = exc_info.value.__cause__
    assert isinstance(cause, ApplicationError)
    assert cause.type == "BadAmount"

    # The rejected update left no trace in workflow state.
    assert dispatcher.query_workflow("counter", "current") == 8

    dispatcher.signal_workflow("counter", "finish")
    assert dispatcher.workflow_result(handle) == 8


@pytest.mark.usefixtures("tdb")
def test_gather_with_retry_exhaustion() -> None:
    """§4.3 test 3: gather of three activities; one fails through its retry
    policy and surfaces as ActivityError(cause=ApplicationError)."""
    attempt_counts.clear()
    dispatcher.register_worker(
        workflows=[GatherWorkflow], activities=[compose, always_fails]
    )
    handle = dispatcher.start_workflow(GatherWorkflow, ["k1"], workflow_id="gather")
    result = dispatcher.workflow_result(handle)
    assert result["ok_results"] == ["hello-g-1", "hello-g-2"]
    assert result["error_type"] == "ActivityError"
    assert result["activity_type"] == "always_fails"
    assert result["retry_state"] == RetryState.MAXIMUM_ATTEMPTS_REACHED.name
    assert result["cause_type"] == "ValueError"
    assert result["cause_message"] == "boom 3"
    assert attempt_counts["k1"] == 3


@pytest.mark.usefixtures("tdb")
def test_buggy_workflow_stays_running_until_fixed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§4.3 test 6: a non-failure exception fails the workflow task, not the
    workflow; swapping in a fixed implementation lets it complete."""
    monkeypatch.setenv(dispatcher.TASK_RETRY_INITIAL_ENV, "0.2")

    @workflow.defn(name="Buggy")
    class BuggyV1:
        @workflow.run
        async def run(self) -> str:
            raise RuntimeError("a bug, not a workflow failure")

    dispatcher.register_worker(workflows=[BuggyV1])
    handle = dispatcher.start_workflow("Buggy", [], workflow_id="buggy")

    # The bug throws almost immediately; the workflow must stay PENDING
    # (Temporal: RUNNING) through task-failure retries.
    import time

    time.sleep(0.5)
    assert dispatcher.workflow_status("buggy") == "PENDING"

    @workflow.defn(name="Buggy")
    class BuggyV2:
        @workflow.run
        async def run(self) -> str:
            return "fixed"

    dispatcher.register_worker(workflows=[BuggyV2])
    assert dispatcher.workflow_result(handle) == "fixed"


@pytest.mark.usefixtures("tdb")
def test_failure_exception_types_fail_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Types listed in failure_exception_types convert to workflow failures
    instead of task retries."""

    @workflow.defn(name="FailsProperly", failure_exception_types=[RuntimeError])
    class FailsProperly:
        @workflow.run
        async def run(self) -> str:
            raise RuntimeError("fail the workflow")

    dispatcher.register_worker(workflows=[FailsProperly])
    handle = dispatcher.start_workflow("FailsProperly", [], workflow_id="fails")
    with pytest.raises(Exception) as exc_info:
        dispatcher.workflow_result(handle)
    assert "fail the workflow" in str(exc_info.value)
    assert dispatcher.workflow_status("fails") == "ERROR"


@pytest.mark.usefixtures("tdb")
def test_deterministic_helpers_run() -> None:
    dispatcher.register_worker(workflows=[DeterminismProbe])
    handle = dispatcher.start_workflow("DeterminismProbe", [], workflow_id="probe")
    values = dispatcher.workflow_result(handle)
    assert len(values) == 3 and all(isinstance(v, str) for v in values)


@pytest.mark.usefixtures("tdb")
def test_workflow_wait_and_as_completed() -> None:
    """workflow.wait returns deterministic input-order *lists* (not sets),
    and as_completed yields awaitables in completion order."""
    dispatcher.register_worker(workflows=[WaitRaceWorkflow], activities=[slow_compose])
    handle = dispatcher.start_workflow(WaitRaceWorkflow, [], workflow_id="wait-race")
    result = dispatcher.workflow_result(handle)
    assert result == {
        "first_done": "timer",
        "done_type": "list",
        "pending_count": 1,
        "slow_result": "hello-slow-1",
    }
