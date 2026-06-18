"""Worker tuning options honored onto DBOS (DEVIATIONS D34):
max_concurrent_activities (semaphore), identity (executor_id), and
activity_executor (sync-activity thread pool)."""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "worker-tuning-tq"

# In-process concurrency tracker (activities run as steps in this process).
_lock = threading.Lock()
_concurrent = 0
_max_concurrent = 0


def _reset_tracker() -> None:
    global _concurrent, _max_concurrent
    with _lock:
        _concurrent = 0
        _max_concurrent = 0


@activity.defn
def tracked_activity() -> str:
    global _concurrent, _max_concurrent
    with _lock:
        _concurrent += 1
        _max_concurrent = max(_max_concurrent, _concurrent)
    time.sleep(0.2)
    with _lock:
        _concurrent -= 1
    return threading.current_thread().name


@workflow.defn
class FanOut:
    @workflow.run
    async def run(self, n: int) -> List[str]:
        results: List[str] = await asyncio.gather(
            *[
                workflow.execute_activity(
                    tracked_activity, start_to_close_timeout=timedelta(seconds=30)
                )
                for _ in range(n)
            ]
        )
        return results


@asynccontextmanager
async def _env(
    *,
    max_concurrent_activities: Optional[int] = None,
    identity: Optional[str] = None,
    activity_executor: Optional[ThreadPoolExecutor] = None,
) -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[FanOut],
        activities=[tracked_activity],
        max_concurrent_activities=max_concurrent_activities,
        identity=identity,
        activity_executor=activity_executor,
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def test_max_concurrent_activities_caps_execution() -> None:
    _reset_tracker()
    async with _env(max_concurrent_activities=2) as client:
        await client.execute_workflow(
            FanOut.run, 6, id="tuning-concurrency", task_queue=TASK_QUEUE
        )
    # Six activities fanned out, but at most two ran at once.
    assert _max_concurrent <= 2
    assert _max_concurrent >= 1


async def test_identity_maps_to_executor_id() -> None:
    async with _env(identity="tdb-custom-identity") as client:
        await client.execute_workflow(
            FanOut.run, 1, id="tuning-identity", task_queue=TASK_QUEUE
        )
    probe = DBOSClient(system_database_url=system_database_url())
    try:
        status = probe.retrieve_workflow("tuning-identity").get_status()
    finally:
        probe.destroy()
    assert status.executor_id == "tdb-custom-identity"


async def test_activity_executor_runs_sync_activities() -> None:
    _reset_tracker()
    executor = ThreadPoolExecutor(thread_name_prefix="tdb-actexec")
    try:
        async with _env(activity_executor=executor) as client:
            names: List[str] = await client.execute_workflow(
                FanOut.run, 2, id="tuning-executor", task_queue=TASK_QUEUE
            )
    finally:
        executor.shutdown(wait=True)
    # Each sync activity ran on a thread from the provided pool.
    assert names
    assert all(name.startswith("tdb-actexec") for name in names), names
