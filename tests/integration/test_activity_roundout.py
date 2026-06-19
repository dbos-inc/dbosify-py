"""Phase 3 activity round-out: heartbeat-delivered cancellation (sync
activities), WAIT_CANCELLATION_COMPLETED, heartbeat details across retry
attempts, and async activity completion.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, AsyncIterator, List, Optional, Sequence

import pytest
from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import (
    AsyncActivityCancelledError,
    Client,
    WorkflowFailureError,
)
from dbosify.common import RetryPolicy
from dbosify.exceptions import (
    ActivityError,
    ApplicationError,
    CancelledError,
    TimeoutError,
    TimeoutType,
)
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "act-roundout-tq"

# In-process mailbox for async-completion task tokens (test and worker share
# the process).
TOKENS: List[bytes] = []


@activity.defn
def heartbeating_forever(path: str) -> None:
    try:
        while True:
            activity.heartbeat()
            time.sleep(0.05)
    except CancelledError:
        with open(path, "a") as f:
            f.write("observed\n")
        raise


@activity.defn
def record(path: str, marker: str) -> None:
    with open(path, "a") as f:
        f.write(f"{marker}\n")


@activity.defn
def flaky_with_heartbeat(path: str) -> Sequence[Any]:
    info = activity.info()
    if info.attempt == 1:
        activity.heartbeat("progress-1")
        raise ApplicationError("try again", type="Flaky")
    return list(info.heartbeat_details)


@activity.defn
async def complete_externally() -> str:
    TOKENS.append(activity.info().task_token)
    activity.raise_complete_async()


@activity.defn
def stalls_after_one_heartbeat() -> Sequence[Any]:
    info = activity.info()
    if info.attempt == 1:
        activity.heartbeat("p1")
        # Stop heartbeating: the watchdog must fail this attempt with
        # TimeoutType.HEARTBEAT. The wait returns once the watchdog marks
        # the attempt cancelled, letting the thread exit promptly.
        activity.wait_for_cancelled_sync(30)
        raise CancelledError("unwound")
    return list(info.heartbeat_details)


@activity.defn
async def slow_writer(path: str) -> None:
    for i in range(100):
        with open(path, "a") as f:
            f.write(f"{i}\n")
        await asyncio.sleep(0.05)


@activity.defn
async def swallow_cancel_and_return() -> str:
    """Mirrors temporalio's ``wait_cancel``: ignore cancellation and return a
    value. An authoritative start-to-close must time out anyway — the late
    return is discarded, not recorded as a success."""
    try:
        while True:
            await asyncio.sleep(0.2)
            activity.heartbeat()
    except asyncio.CancelledError:
        return "swallowed-and-returned"


@workflow.defn
class CancelStopsAsyncFnWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        handle = workflow.start_activity(
            slow_writer,
            path,
            start_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=30),  # watchdog armed, idle
        )
        await workflow.sleep(0.4)
        handle.cancel()
        try:
            await handle
        except asyncio.CancelledError:
            pass
        return "done"


@activity.defn
def heartbeating_with_marker(path: str) -> None:
    with open(path, "a") as f:
        f.write("started\n")
    try:
        while True:
            activity.heartbeat()
            time.sleep(0.05)
    except CancelledError:
        with open(path, "a") as f:
            f.write("observed\n")
        raise


@workflow.defn
class OrphanAtCloseWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self, path: str) -> str:
        # Start but never await: the workflow closes (on the test's signal,
        # sent only once the activity has provably started) with the
        # threaded, heartbeating attempt still running.
        workflow.start_activity(
            heartbeating_with_marker,
            path,
            start_to_close_timeout=timedelta(seconds=60),
        )
        await workflow.wait_condition(lambda: self.done)
        return "closed"


@workflow.defn
class CancellationWorkflow:
    @workflow.run
    async def run(self, path: str) -> None:
        try:
            await workflow.execute_activity(
                heartbeating_forever,
                path,
                start_to_close_timeout=timedelta(seconds=60),
            )
        finally:
            await workflow.execute_activity(
                record,
                args=[path, "cleanup"],
                start_to_close_timeout=timedelta(seconds=10),
            )


@workflow.defn
class WaitCancelWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        handle = workflow.start_activity(
            heartbeating_forever,
            path,
            start_to_close_timeout=timedelta(seconds=60),
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        await workflow.sleep(0.5)
        handle.cancel()
        try:
            await handle
        except asyncio.CancelledError:
            pass
        # WAIT means we only get here after the activity confirmed its
        # cancellation — "observed" must precede "after" in the effects.
        await workflow.execute_activity(
            record, args=[path, "after"], start_to_close_timeout=timedelta(seconds=10)
        )
        return "done"


@workflow.defn
class HeartbeatDetailsWorkflow:
    @workflow.run
    async def run(self, path: str) -> Sequence[Any]:
        result: Sequence[Any] = await workflow.execute_activity(
            flaky_with_heartbeat,
            path,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50), maximum_attempts=2
            ),
        )
        return result


@workflow.defn
class HeartbeatTimeoutWorkflow:
    @workflow.run
    async def run(self) -> Sequence[Any]:
        result: Sequence[Any] = await workflow.execute_activity(
            stalls_after_one_heartbeat,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=0.4),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50), maximum_attempts=2
            ),
        )
        return result


@workflow.defn
class AsyncRetryWorkflow:
    @workflow.run
    async def run(self) -> str:
        result: str = await workflow.execute_activity(
            complete_externally,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50), maximum_attempts=2
            ),
        )
        return result


@workflow.defn
class AsyncTimeoutWorkflow:
    @workflow.run
    async def run(self, s2c: float, hb: Optional[float], max_attempts: int = 1) -> str:
        result: str = await workflow.execute_activity(
            complete_externally,
            start_to_close_timeout=timedelta(seconds=s2c),
            heartbeat_timeout=timedelta(seconds=hb) if hb else None,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50),
                maximum_attempts=max_attempts,
            ),
        )
        return result


@workflow.defn
class SwallowedCancelTimeoutWorkflow:
    @workflow.run
    async def run(self, local: bool) -> str:
        execute = (
            workflow.execute_local_activity if local else workflow.execute_activity
        )
        result: str = await execute(
            swallow_cancel_and_return,
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result


@workflow.defn
class DuplicateIdWorkflow:
    @workflow.run
    async def run(self) -> None:
        workflow.start_activity(
            heartbeating_forever,
            "unused",
            start_to_close_timeout=timedelta(seconds=60),
            activity_id="same-id",
        )
        workflow.start_activity(
            heartbeating_forever,
            "unused",
            start_to_close_timeout=timedelta(seconds=60),
            activity_id="same-id",
        )


@workflow.defn
class AsyncCustomIdWorkflow:
    @workflow.run
    async def run(self) -> str:
        result: str = await workflow.execute_activity(
            complete_externally,
            start_to_close_timeout=timedelta(seconds=60),
            activity_id="my-custom-act",
        )
        return result


@workflow.defn
class AsyncCanWorkflow:
    @workflow.signal
    def hop_now(self) -> None:
        workflow.continue_as_new(True)

    @workflow.run
    async def run(self, hopped: bool) -> str:
        result: str = await workflow.execute_activity(
            complete_externally, start_to_close_timeout=timedelta(seconds=60)
        )
        return result


@workflow.defn
class AsyncCompleteWorkflow:
    @workflow.run
    async def run(self, catch_cancel: bool) -> Any:
        try:
            return await workflow.execute_activity(
                complete_externally, start_to_close_timeout=timedelta(seconds=60)
            )
        except asyncio.CancelledError:
            if not catch_cancel:
                raise
            return "reported-cancelled"


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            CancellationWorkflow,
            WaitCancelWorkflow,
            HeartbeatDetailsWorkflow,
            AsyncCompleteWorkflow,
            HeartbeatTimeoutWorkflow,
            AsyncRetryWorkflow,
            AsyncTimeoutWorkflow,
            AsyncCustomIdWorkflow,
            DuplicateIdWorkflow,
            AsyncCanWorkflow,
            CancelStopsAsyncFnWorkflow,
            OrphanAtCloseWorkflow,
            SwallowedCancelTimeoutWorkflow,
        ],
        activities=[
            heartbeating_forever,
            heartbeating_with_marker,
            slow_writer,
            record,
            flaky_with_heartbeat,
            complete_externally,
            stalls_after_one_heartbeat,
            swallow_cancel_and_return,
        ],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            TOKENS.clear()
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def _wait_for_file_line(path: Path, line: str, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if path.exists() and line in path.read_text().splitlines():
            return
        assert asyncio.get_running_loop().time() < deadline, f"never saw {line!r}"
        await asyncio.sleep(0.1)


async def test_sync_activity_observes_cancellation_via_heartbeat(
    tmp_path: Path,
) -> None:
    """Workflow cancel reaches a sync (threaded) activity at its next
    heartbeat, which raises CancelledError inside the activity; cleanup
    activities still run during the workflow's unwind."""
    effects = tmp_path / "effects"
    async with _env() as client:
        handle = await client.start_workflow(
            CancellationWorkflow.run,
            str(effects),
            id="hb-cancel",
            task_queue=TASK_QUEUE,
        )
        await asyncio.sleep(0.5)  # let the activity start heartbeating
        await handle.cancel()
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        await _wait_for_file_line(effects, "cleanup")
        # The abandoned activity thread observes cancellation and unwinds.
        await _wait_for_file_line(effects, "observed")


async def test_wait_cancellation_completed(tmp_path: Path) -> None:
    """WAIT_CANCELLATION_COMPLETED: the await resolves only after the
    activity confirms its cancellation — its unwind strictly precedes the
    workflow's next step."""
    effects = tmp_path / "effects"
    async with _env() as client:
        result = await client.execute_workflow(
            WaitCancelWorkflow.run,
            str(effects),
            id="wait-cancel",
            task_queue=TASK_QUEUE,
        )
        assert result == "done"
    assert effects.read_text().splitlines() == ["observed", "after"]


async def test_heartbeat_details_reach_next_attempt(tmp_path: Path) -> None:
    """Heartbeat details from a failed attempt surface on the next
    attempt's info().heartbeat_details (in-process)."""
    async with _env() as client:
        result = await client.execute_workflow(
            HeartbeatDetailsWorkflow.run,
            str(tmp_path / "unused"),
            id="hb-details",
            task_queue=TASK_QUEUE,
        )
        assert result == ["progress-1"]


async def _token(timeout: float = 10.0) -> bytes:
    deadline = asyncio.get_running_loop().time() + timeout
    while not TOKENS:
        assert asyncio.get_running_loop().time() < deadline, "no task token"
        await asyncio.sleep(0.05)
    return TOKENS[-1]


async def test_async_activity_completion() -> None:
    """raise_complete_async parks the activity; an external client completes
    it by task token (heartbeating along the way) and the workflow gets the
    result."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncCompleteWorkflow.run, False, id="async-done", task_queue=TASK_QUEUE
        )
        async_handle = client.get_async_activity_handle(task_token=await _token())
        await async_handle.heartbeat("almost")
        await async_handle.complete("externally-done")
        assert await handle.result() == "externally-done"


async def test_async_activity_fail() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncCompleteWorkflow.run, False, id="async-fail", task_queue=TASK_QUEUE
        )
        async_handle = client.get_async_activity_handle(task_token=await _token())
        await async_handle.fail(
            ApplicationError("boom", type="Boom", non_retryable=True)
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        cause = exc_info.value.cause
        assert isinstance(cause, ActivityError)
        assert isinstance(cause.__cause__, ApplicationError)
        assert cause.__cause__.type == "Boom"


async def test_async_activity_report_cancellation() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncCompleteWorkflow.run, True, id="async-cancel", task_queue=TASK_QUEUE
        )
        async_handle = client.get_async_activity_handle(task_token=await _token())
        await async_handle.report_cancellation()
        assert await handle.result() == "reported-cancelled"


@pytest.mark.usefixtures("cleanup_test_databases")
def test_async_activity_completion_survives_sigkill(tmp_path: Path) -> None:
    """SIGKILL while an activity is parked awaiting external completion:
    recovery re-parks it from the checkpointed async-pending marker without
    re-running the activity function, and a post-recovery external
    completion still resolves the workflow."""
    from tests.harness import PythonProcess

    worker_script = Path(__file__).parent / "phase3_worker.py"
    env = {
        "PYTHONPATH": str(Path(__file__).parents[2]),
    }
    token_file = tmp_path / "token"
    wf_id = "asyncact-wf"

    first = PythonProcess(
        worker_script, "asyncact-start", wf_id, str(token_file), env=env
    )
    first.start()
    try:
        first.wait_for_line("TOKEN_WRITTEN", timeout=60)
        # Let the attempt step's async-pending checkpoint commit so recovery
        # re-parks (rather than re-running the function).
        time.sleep(1.0)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(
        worker_script, "asyncact-resume", wf_id, str(token_file), env=env
    )
    second.start()
    try:
        second.wait_for_line("STARTED", timeout=60)
        token = token_file.read_text().encode()
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            asyncio.run(
                client.get_async_activity_handle(task_token=token).complete("recovered")
            )
        finally:
            dbos_client.destroy()
        import json

        result = json.loads(
            second.wait_for_line("RESULT ", timeout=60).split("RESULT ", 1)[1]
        )
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == {"result": "recovered", "status": "COMPLETED"}
    # The activity function ran exactly once (the replay re-parked from the
    # checkpointed marker, it did not re-execute the function).
    assert not [l for l in second.transcript if "TOKEN_WRITTEN" in l]


async def test_heartbeat_timeout_fails_attempt_and_retries() -> None:
    """An attempt that stops heartbeating fails with TimeoutType.HEARTBEAT
    and retries; the next attempt sees the stalled attempt's last heartbeat
    details."""
    async with _env() as client:
        result = await client.execute_workflow(
            HeartbeatTimeoutWorkflow.run, id="hb-timeout", task_queue=TASK_QUEUE
        )
        assert result == ["p1"]


async def test_async_activity_fail_retries() -> None:
    """An external fail() consults the retry policy: the next attempt
    re-runs the activity function (a fresh async park under the same
    token), as in Temporal."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncRetryWorkflow.run, id="async-retry", task_queue=TASK_QUEUE
        )
        first_token = await _token()
        await client.get_async_activity_handle(task_token=first_token).fail(
            ApplicationError("transient", type="Flaky")
        )
        deadline = asyncio.get_running_loop().time() + 10
        while len(TOKENS) < 2:
            assert asyncio.get_running_loop().time() < deadline, "no retry attempt"
            await asyncio.sleep(0.05)
        await client.get_async_activity_handle(task_token=TOKENS[-1]).complete(
            "second-attempt"
        )
        assert await handle.result() == "second-attempt"


def _timeout_cause(exc_info: Any) -> TimeoutError:
    cause = exc_info.value.cause
    assert isinstance(cause, ActivityError)
    assert isinstance(cause.__cause__, TimeoutError)
    return cause.__cause__


@pytest.mark.parametrize("local", [False, True], ids=["queued", "local"])
async def test_start_to_close_is_authoritative_over_swallowed_cancel(
    local: bool,
) -> None:
    """An activity that catches CancelledError and returns a value must still
    time out: the start-to-close deadline is authoritative and discards the late
    return rather than recording a success. Covers both the queued
    (execute_activity) and local (execute_local_activity) paths, which share the
    attempt step. Regression for the cooperative-timeout deviation."""
    async with _env() as client:
        handle = await client.start_workflow(
            SwallowedCancelTimeoutWorkflow.run,
            local,
            id=f"swallow-s2c-{'local' if local else 'queued'}",
            task_queue=TASK_QUEUE,
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        assert _timeout_cause(exc_info).type == TimeoutType.START_TO_CLOSE


async def test_parked_async_activity_start_to_close() -> None:
    """start-to-close keeps applying while an activity is parked awaiting
    external completion (Temporal semantics)."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncTimeoutWorkflow.run,
            args=[0.8, None],
            id="parked-s2c",
            task_queue=TASK_QUEUE,
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        assert _timeout_cause(exc_info).type == TimeoutType.START_TO_CLOSE


async def test_parked_async_activity_heartbeat_timeout() -> None:
    """A parked async activity whose completer never heartbeats times out
    with TimeoutType.HEARTBEAT; one that heartbeats inside the window
    survives and completes."""
    async with _env() as client:
        silent = await client.start_workflow(
            AsyncTimeoutWorkflow.run,
            args=[30.0, 0.5],
            id="parked-hb-silent",
            task_queue=TASK_QUEUE,
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await silent.result()
        assert _timeout_cause(exc_info).type == TimeoutType.HEARTBEAT

        TOKENS.clear()
        alive = await client.start_workflow(
            AsyncTimeoutWorkflow.run,
            args=[30.0, 0.8],
            id="parked-hb-alive",
            task_queue=TASK_QUEUE,
        )
        async_handle = client.get_async_activity_handle(task_token=await _token())
        for _ in range(4):
            await async_handle.heartbeat("alive")
            await asyncio.sleep(0.3)
        await async_handle.complete("kept-alive")
        assert await alive.result() == "kept-alive"


async def test_completer_learns_of_cancellation() -> None:
    """When the workflow side cancels a parked async activity, the
    completer's next heartbeat/complete raises AsyncActivityCancelledError
    instead of delivering into the void."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncCompleteWorkflow.run, False, id="async-gone", task_queue=TASK_QUEUE
        )
        async_handle = client.get_async_activity_handle(task_token=await _token())
        await handle.cancel()
        deadline = asyncio.get_running_loop().time() + 10
        while True:
            try:
                await async_handle.heartbeat("still here?")
            except AsyncActivityCancelledError:
                break
            assert asyncio.get_running_loop().time() < deadline, "never marked gone"
            await asyncio.sleep(0.1)
        with pytest.raises(AsyncActivityCancelledError):
            await async_handle.complete("too late")
        # The acknowledgment itself must never raise (canonical pattern:
        # heartbeat raises -> report_cancellation confirms).
        await async_handle.report_cancellation()


async def test_async_activity_reference_addressing() -> None:
    """get_async_activity_handle by workflow_id + activity_id (no token, no
    run_id: the chain's current run is resolved)."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncCustomIdWorkflow.run, id="async-by-ref", task_queue=TASK_QUEUE
        )
        await _token()  # wait until the activity has parked
        async_handle = client.get_async_activity_handle(
            workflow_id="async-by-ref", activity_id="my-custom-act"
        )
        await async_handle.complete("by-reference")
        assert await handle.result() == "by-reference"


async def test_parked_heartbeat_timeout_retries_then_completes() -> None:
    """A parked heartbeat timeout consults the retry policy: the function
    re-runs, parks again, and a live completer finishes the second
    attempt."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncTimeoutWorkflow.run,
            args=[30.0, 0.4, 2],
            id="parked-hb-retry",
            task_queue=TASK_QUEUE,
        )
        await _token()
        deadline = asyncio.get_running_loop().time() + 15
        while len(TOKENS) < 2:  # attempt 2's park
            assert asyncio.get_running_loop().time() < deadline, "no retry"
            await asyncio.sleep(0.1)
        await client.get_async_activity_handle(task_token=TOKENS[-1]).complete(
            "second-attempt"
        )
        assert await handle.result() == "second-attempt"


async def test_duplicate_open_activity_id_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate open activity ids are rejected (Temporal's server rejects
    the command; here, like any rejected command, it fails the workflow
    task — surfaced via FAIL_FAST for the test)."""
    monkeypatch.setenv("DBOSIFY_FAIL_FAST", "1")
    async with _env() as client:
        handle = await client.start_workflow(
            DuplicateIdWorkflow.run, id="dup-act-id", task_queue=TASK_QUEUE
        )
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        assert "already in use" in str(exc_info.value.cause)


async def test_async_completion_does_not_cross_continue_as_new() -> None:
    """A stale completion addressed to the old run's parked activity must
    not ride carryover into the new run (whose own activities reuse the
    same default ids) — and the old run's completer learns its activity is
    gone."""
    async with _env() as client:
        handle = await client.start_workflow(
            AsyncCanWorkflow.run, False, id="async-can", task_queue=TASK_QUEUE
        )
        old_token = await _token()
        old_handle = client.get_async_activity_handle(task_token=old_token)
        # Hop first, then race a stale completion in behind it: FIFO puts it
        # in the old run's inbox, where the carryover drain must drop it.
        await handle.signal(AsyncCanWorkflow.hop_now)
        try:
            await old_handle.complete("STALE")
        except AsyncActivityCancelledError:
            pass  # the close already marked it gone: equally correct
        # The new run parks its own activity (same default id, fresh token).
        deadline = asyncio.get_running_loop().time() + 15
        while len(TOKENS) < 2:
            assert asyncio.get_running_loop().time() < deadline, "no new-run park"
            await asyncio.sleep(0.1)
        await client.get_async_activity_handle(task_token=TOKENS[-1]).complete("fresh")
        # A misdelivered stale envelope would have produced "STALE".
        assert await handle.result() == "fresh"
        # The old run's activity is marked gone for its completer.
        with pytest.raises(AsyncActivityCancelledError):
            await old_handle.heartbeat("anyone?")


async def test_cancel_stops_async_activity_function(tmp_path: Path) -> None:
    """TRY_CANCEL must actually cancel an async activity function even when
    the heartbeat watchdog wraps it (asyncio.wait does not propagate
    cancellation to what it waits on — the attempt must)."""
    effects = tmp_path / "effects"
    async with _env() as client:
        result = await client.execute_workflow(
            CancelStopsAsyncFnWorkflow.run,
            str(effects),
            id="cancel-stops-fn",
            task_queue=TASK_QUEUE,
        )
        assert result == "done"
        lines_at_done = len(effects.read_text().splitlines())
        await asyncio.sleep(0.8)
        # An orphaned function would still be appending (~16 more lines).
        assert len(effects.read_text().splitlines()) <= lines_at_done + 2


async def test_close_unwinds_orphaned_activities(tmp_path: Path) -> None:
    """A workflow closing with a still-running activity marks it cancelled
    (its thread unwinds at the next heartbeat instead of spinning forever)
    and drops its cross-attempt worker state."""
    effects = tmp_path / "effects"
    async with _env() as client:
        handle = await client.start_workflow(
            OrphanAtCloseWorkflow.run,
            str(effects),
            id="orphan-close",
            task_queue=TASK_QUEUE,
        )
        # Only close the workflow once the activity is provably running
        # (on a slow runner the attempt might otherwise be torn down before
        # its function ever starts — correct, but not the path under test).
        await _wait_for_file_line(effects, "started")
        await handle.signal(OrphanAtCloseWorkflow.finish)
        assert await handle.result() == "closed"
        # The orphaned thread observes the close-time cancel.
        await _wait_for_file_line(effects, "observed")
        leaked = [k for k in activity._heartbeat_store if k[0] == "orphan-close"]
        assert not leaked


async def test_cancel_raises_on_closed_run(tmp_path: Path) -> None:
    """cancel() on an already-closed run raises (Temporal's
    already-completed semantics) instead of silently sending into a dead
    inbox."""
    async with _env() as client:
        handle = await client.start_workflow(
            OrphanAtCloseWorkflow.run,
            str(tmp_path / "effects"),
            id="cancel-closed",
            task_queue=TASK_QUEUE,
        )
        await handle.signal(OrphanAtCloseWorkflow.finish)
        await handle.result()
        with pytest.raises(RuntimeError, match="already closed"):
            await handle.cancel()
