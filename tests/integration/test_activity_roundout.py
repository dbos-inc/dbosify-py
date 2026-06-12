"""Phase 3 activity round-out: heartbeat-delivered cancellation (sync
activities), WAIT_CANCELLATION_COMPLETED, heartbeat details across retry
attempts, and async activity completion.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, AsyncIterator, List, Sequence

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client, WorkflowFailureError
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import ActivityError, ApplicationError, CancelledError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

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
        ],
        activities=[
            heartbeating_forever,
            record,
            flaky_with_heartbeat,
            complete_externally,
            stalls_after_one_heartbeat,
        ],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            TOKENS.clear()
            yield await Client.connect(dbos_client)
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
        "TEMPORAL_DBOS_CLIENT_POLL_SECONDS": "0.05",
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
            client = asyncio.run(Client.connect(dbos_client))
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
