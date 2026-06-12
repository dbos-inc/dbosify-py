"""The §6.5 cancellation matrix, in-process: cooperative cancel (parked,
swallowed, cleanup-during-unwind, mid-activity), forceful terminate, and the
TERMINATE_EXISTING conflict policy. The SIGKILL-during-unwind recovery test
lives in test_cancellation_recovery.py.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import AsyncIterator, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporal_dbos.common import WorkflowIDConflictPolicy
from temporal_dbos.exceptions import CancelledError, TerminatedError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "phase2-cancel-tq"


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
class CleanupWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        try:
            await workflow.wait_condition(lambda: False)
        finally:
            # The load-bearing §6.5 row: cleanup during cancellation unwind
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
        workflows=[ParkedWorkflow, SwallowingWorkflow, CleanupWorkflow, BusyWorkflow],
        activities=[record, slow_activity],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


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
