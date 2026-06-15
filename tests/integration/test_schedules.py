"""Schedules (DESIGN §6.7): create/describe/list/update/pause/trigger/backfill/
delete against real Postgres, plus an automatic cron fire and persistence
across a worker restart.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, List, Optional

import pytest
from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleBackfill,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

pytestmark = pytest.mark.usefixtures("tdb_env")

TASK_QUEUE = "sched-tq"
ACTION_WF_NAME = "wf:ScheduledGreeter"


@dataclass
class Params:
    greeting: str
    name: str


@activity.defn
async def greet(p: Params) -> str:
    return f"{p.greeting}, {p.name}!"


@workflow.defn
class ScheduledGreeter:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            greet, Params("Hello", name), start_to_close_timeout=timedelta(seconds=10)
        )


def _fast_scheduler_config() -> dict:
    config = dict(default_config())
    # Make the dynamic scheduler poll quickly so automatic cron fires happen
    # within the test timeout.
    config["runtimeConfig"] = {"scheduler_polling_interval_sec": 0.1}
    return config


@asynccontextmanager
async def _env(config: Optional[dict] = None) -> AsyncIterator[Client]:
    worker = Worker(
        config or default_config(),
        task_queue=TASK_QUEUE,
        workflows=[ScheduledGreeter],
        activities=[greet],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def _wait_for_action(
    client: Client, *, name: str = "World", timeout: float = 15.0
) -> str:
    """Wait for a scheduled action workflow to complete and return its result."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = await client._dbos_client.list_workflows_async(name=ACTION_WF_NAME)
        for row in rows:
            if row.status == "SUCCESS":
                handle = client.get_workflow_handle(row.workflow_id, result_type=str)
                return await handle.result()
        await asyncio.sleep(0.1)
    pytest.fail("scheduled action workflow never completed")


def _interval_schedule(arg: str = "World") -> Schedule:
    return Schedule(
        action=ScheduleActionStartWorkflow(
            ScheduledGreeter.run,
            arg,
            id="greeter-wf",
            task_queue=TASK_QUEUE,
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=2))]),
        state=ScheduleState(note="a note"),
    )


async def test_create_describe_list_delete() -> None:
    async with _env() as client:
        handle = await client.create_schedule("sched-1", _interval_schedule())
        assert handle.id == "sched-1"

        desc = await handle.describe()
        assert desc.id == "sched-1"
        assert desc.schedule.state.note == "a note"
        assert isinstance(desc.schedule.action, ScheduleActionStartWorkflow)
        assert desc.schedule.action.workflow == "ScheduledGreeter"
        # next_action_times computed from the compiled cron.
        assert len(desc.info.next_action_times) > 0

        ids = [s.id async for s in await client.list_schedules()]
        assert "sched-1" in ids

        await handle.delete()
        ids_after = [s.id async for s in await client.list_schedules()]
        assert "sched-1" not in ids_after


async def test_trigger_runs_action() -> None:
    async with _env() as client:
        handle = await client.create_schedule("sched-trigger", _interval_schedule())
        await handle.trigger()
        assert await _wait_for_action(client) == "Hello, World!"
        await handle.delete()


async def test_backfill_runs_actions() -> None:
    async with _env() as client:
        handle = await client.create_schedule("sched-backfill", _interval_schedule())
        now = datetime.now(timezone.utc)
        await handle.backfill(
            ScheduleBackfill(
                start_at=now - timedelta(minutes=10),
                end_at=now - timedelta(minutes=4),
                overlap=ScheduleOverlapPolicy.ALLOW_ALL,
            )
        )
        assert await _wait_for_action(client) == "Hello, World!"
        await handle.delete()


async def test_pause_and_unpause() -> None:
    async with _env() as client:
        handle = await client.create_schedule("sched-pause", _interval_schedule())
        await handle.pause(note="paused now")
        desc = await handle.describe()
        assert desc.schedule.state.paused is True
        assert desc.schedule.state.note == "paused now"

        await handle.unpause(note="back on")
        desc = await handle.describe()
        assert desc.schedule.state.paused is False
        assert desc.schedule.state.note == "back on"
        await handle.delete()


async def test_update_changes_args() -> None:
    async with _env() as client:
        handle = await client.create_schedule("sched-update", _interval_schedule())

        def updater(inp: ScheduleUpdateInput) -> ScheduleUpdate:
            action = inp.description.schedule.action
            assert isinstance(action, ScheduleActionStartWorkflow)
            action.args = ["Updated"]
            return ScheduleUpdate(schedule=inp.description.schedule)

        await handle.update(updater)
        await handle.trigger()
        assert await _wait_for_action(client, name="Updated") == "Hello, Updated!"
        await handle.delete()


async def test_automatic_cron_fire() -> None:
    async with _env(_fast_scheduler_config()) as client:
        schedule = Schedule(
            action=ScheduleActionStartWorkflow(
                ScheduledGreeter.run,
                "Auto",
                id="auto-wf",
                task_queue=TASK_QUEUE,
            ),
            # Every second (compiles to a 6-field seconds cron).
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]
            ),
        )
        handle = await client.create_schedule("sched-auto", schedule)
        assert await _wait_for_action(client) == "Hello, Auto!"
        await handle.delete()


async def test_schedule_persists_across_worker_restart() -> None:
    # Create the schedule under one worker, then run a fresh worker (DBOS
    # destroy/relaunch) and confirm the persisted row still fires on trigger.
    async with _env() as client:
        await client.create_schedule("sched-persist", _interval_schedule())

    from temporal_dbos import worker as _worker_mod

    _worker_mod._reset_for_tests()

    async with _env() as client:
        ids = [s.id async for s in await client.list_schedules()]
        assert "sched-persist" in ids
        handle = client.get_schedule_handle("sched-persist")
        await handle.trigger()
        assert await _wait_for_action(client) == "Hello, World!"
        await handle.delete()
