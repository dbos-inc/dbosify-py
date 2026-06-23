"""The cancellation matrix, in-process: cooperative cancel (parked,
swallowed, cleanup-during-unwind, mid-activity), forceful terminate, and the
TERMINATE_EXISTING conflict policy. The SIGKILL-during-unwind recovery test
lives in test_cancellation_recovery.py.
"""

import asyncio
import sys
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import AsyncIterator, Optional

import pytest

from dbosify import activity, workflow
from dbosify.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
)
from dbosify.common import WorkflowIDConflictPolicy
from dbosify.exceptions import CancelledError, TerminatedError
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

pytestmark = pytest.mark.usefixtures("dbosify_env")

TASK_QUEUE = "cancel-tq"


@activity.defn
async def record(path: str, label: str) -> str:
    with open(path, "a") as f:
        f.write(label + "\n")
    return label


@activity.defn
async def slow_activity(path: str) -> str:
    with open(path, "a") as f:
        f.write("started\n")
    await asyncio.sleep(30)
    return "done"


@workflow.defn
class ParkedWorkflow:
    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: False)
        return "unreachable"


@workflow.defn
class SwallowingWorkflow:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.wait_condition(lambda: False)
        except asyncio.CancelledError:
            # Swallowing a cancel is legal in Temporal: COMPLETED.
            return f"survived:{workflow.cancellation_reason()}"
        return "unreachable"


@workflow.defn
class UncancelWorkflow:
    """Exercises the stdlib asyncio.Task cancel-counter (3.11+) on the virtual
    loop: a cancel increments ``cancelling()``; ``uncancel()`` decrements it, so
    the workflow can swallow the cancel (the shield-loop idiom) and complete.
    There is no ``workflow.uncancel`` to add — user code reaches it through the
    real ``asyncio.Task`` the interpreter hosts the run coroutine on.
    """

    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.wait_condition(lambda: False)
        except asyncio.CancelledError:
            task = asyncio.current_task()
            assert task is not None
            # cancelling()/uncancel() are 3.11+; the version guard lets mypy
            # narrow them away on 3.10 (the test is skipped there too).
            if sys.version_info >= (3, 11):
                requested = task.cancelling()
                remaining = task.uncancel()
                return f"cancelling={requested} uncancelled_to={remaining}"
            return "no-uncancel"
        return "unreachable"


@workflow.defn
class CleanupWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        try:
            await workflow.wait_condition(lambda: False)
        finally:
            # The load-bearing row: cleanup during cancellation unwind
            # may still execute activities.
            await workflow.execute_activity(
                record,
                args=[path, "cleanup"],
                start_to_close_timeout=timedelta(seconds=10),
            )
        return "unreachable"


@workflow.defn
class BusyWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        result: str = await workflow.execute_activity(
            slow_activity, path, start_to_close_timeout=timedelta(seconds=60)
        )
        return result


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            ParkedWorkflow,
            SwallowingWorkflow,
            UncancelWorkflow,
            CleanupWorkflow,
            BusyWorkflow,
        ],
        activities=[record, slow_activity],
    )
    async with worker:
        client = await connect_client()
        try:
            yield client
        finally:
            await client.close()


async def _assert_cancelled_result(handle: WorkflowHandle) -> None:
    with pytest.raises(WorkflowFailureError) as exc_info:
        await handle.result()
    assert isinstance(exc_info.value.cause, CancelledError)


async def test_cancel_parked_workflow() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            ParkedWorkflow.run, id="cancel-parked", task_queue=TASK_QUEUE
        )
        await handle.cancel(reason="user asked")
        await _assert_cancelled_result(handle)
        assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED


async def test_swallowed_cancel_completes() -> None:
    async with _env() as client:
        handle = await client.start_workflow(
            SwallowingWorkflow.run, id="cancel-swallow", task_queue=TASK_QUEUE
        )
        await handle.cancel(reason="nope")
        assert await handle.result() == "survived:nope"
        assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="asyncio.Task.uncancel()/cancelling() are 3.11+",
)
async def test_uncancel_clears_cancel_counter() -> None:
    # asyncio.Task.uncancel()/cancelling() work on the interpreter's real tasks:
    # the cooperative cancel injects via task.cancel(), so the native counter is
    # 1 on entry and uncancel() returns it to 0 (the shield-loop idiom).
    async with _env() as client:
        handle = await client.start_workflow(
            UncancelWorkflow.run, id="cancel-uncancel", task_queue=TASK_QUEUE
        )
        await handle.cancel()
        assert await handle.result() == "cancelling=1 uncancelled_to=0"
        assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED


async def test_cleanup_activity_runs_during_unwind(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    async with _env() as client:
        handle = await client.start_workflow(
            CleanupWorkflow.run,
            str(effects),
            id="cancel-cleanup",
            task_queue=TASK_QUEUE,
        )
        await handle.cancel()
        await _assert_cancelled_result(handle)
        assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED
    assert effects.read_text() == "cleanup\n"


async def test_cancel_during_in_flight_activity(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    async with _env() as client:
        handle = await client.start_workflow(
            BusyWorkflow.run, str(effects), id="cancel-busy", task_queue=TASK_QUEUE
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not effects.exists():
            await asyncio.sleep(0.1)
        assert effects.exists(), "activity never started"
        await handle.cancel()
        await _assert_cancelled_result(handle)
        assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED


async def test_terminate_runs_no_workflow_code(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    async with _env() as client:
        handle = await client.start_workflow(
            CleanupWorkflow.run,
            str(effects),
            id="terminate-cleanup",
            task_queue=TASK_QUEUE,
        )
        await handle.terminate(reason="kill it")
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()
        assert isinstance(exc_info.value.cause, TerminatedError)
        assert (await handle.describe()).status == WorkflowExecutionStatus.TERMINATED
    # Forceful termination ran no workflow code: no cleanup activity.
    assert not effects.exists()


async def test_terminate_existing_conflict_policy() -> None:
    async with _env() as client:
        first = await client.start_workflow(
            ParkedWorkflow.run, id="term-existing", task_queue=TASK_QUEUE
        )
        second = await client.start_workflow(
            ParkedWorkflow.run,
            id="term-existing",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING,
        )
        assert second.result_run_id == "term-existing--r1"
        # `first` is unbound (signals/describe follow the chain's current
        # run, as in temporalio); pin the terminated run explicitly.
        first_run = client.get_workflow_handle("term-existing", run_id="term-existing")
        assert (await first_run.describe()).status == (
            WorkflowExecutionStatus.TERMINATED
        )
        assert (await second.describe()).status == WorkflowExecutionStatus.RUNNING
        await second.terminate()
