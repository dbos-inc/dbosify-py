"""Regression: heartbeats issued from a *background asyncio task* inside an
async activity must reset the heartbeat-timeout watchdog, so a long activity
outlives its heartbeat timeout. This is the samples-python ``custom_decorator``
(``@auto_heartbeater``) pattern.
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncIterator

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.common import RetryPolicy
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "hb-bgtask-tq"


async def _beat_forever(delay: float) -> None:
    while True:
        await asyncio.sleep(delay)
        activity.heartbeat()


@activity.defn
async def background_heartbeat_activity() -> str:
    # Mirrors @auto_heartbeater: a background task issues heartbeats so a long
    # activity outlives its heartbeat timeout. The activity itself yields to the
    # loop (sleep), so the background task gets to run.
    hb = activity.info().heartbeat_timeout
    assert hb is not None, "heartbeat_timeout not propagated to activity.info()"
    beater = asyncio.create_task(_beat_forever(hb.total_seconds() / 2))
    try:
        await asyncio.sleep(3.0)  # 3x the heartbeat timeout
        return "completed"
    finally:
        beater.cancel()


@workflow.defn
class BackgroundHeartbeatWorkflow:
    @workflow.run
    async def run(self) -> str:
        result: str = await workflow.execute_activity(
            background_heartbeat_activity,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=1),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result


@activity.defn
async def wait_for_cancel_bg_heartbeat() -> str:
    # Faithful to samples-python custom_decorator: await forever, kept alive by
    # a background heartbeater, until cancelled.
    hb = activity.info().heartbeat_timeout
    beater = asyncio.create_task(_beat_forever(hb.total_seconds() / 2)) if hb else None
    try:
        try:
            await asyncio.Future()
            raise RuntimeError("unreachable")
        except asyncio.CancelledError:
            return "activity cancelled!"
    finally:
        if beater is not None:
            beater.cancel()
            await asyncio.wait([beater])


@workflow.defn
class CancelAfterDelayWorkflow:
    @workflow.run
    async def run(self) -> str:
        handle = workflow.start_activity(
            wait_for_cancel_bg_heartbeat,
            start_to_close_timeout=timedelta(hours=20),
            heartbeat_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        # The activity must survive these 4s on background heartbeats alone
        # (2x the heartbeat timeout) before we cancel it.
        await workflow.sleep(timedelta(seconds=4))
        handle.cancel()
        result: str = await handle
        return result


@asynccontextmanager
async def _env() -> AsyncIterator[Client]:
    worker = Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[BackgroundHeartbeatWorkflow, CancelAfterDelayWorkflow],
        activities=[background_heartbeat_activity, wait_for_cancel_bg_heartbeat],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield Client(dbos_client)
        finally:
            dbos_client.destroy()


async def test_background_task_heartbeats_keep_activity_alive() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            BackgroundHeartbeatWorkflow.run,
            id="bg-hb-wf",
            task_queue=TASK_QUEUE,
        )
        assert result == "completed"


async def test_background_heartbeat_survives_until_cancelled() -> None:
    async with _env() as client:
        result = await client.execute_workflow(
            CancelAfterDelayWorkflow.run,
            id="bg-hb-cancel-wf",
            task_queue=TASK_QUEUE,
        )
        assert result == "activity cancelled!"
