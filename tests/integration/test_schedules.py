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
from dbos import DBOSClient, DBOSConfig

from temporal_dbos import activity, workflow
from temporal_dbos.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleBackfill,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
    WorkflowExecutionStatus,
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
        result: str = await workflow.execute_activity(
            greet, Params("Hello", name), start_to_close_timeout=timedelta(seconds=10)
        )
        return result


# --- overlap-policy probe: counts how many actions actually start ------------
# Activities run in the in-process worker, so they share this module state. We
# count *starts* (not instantaneous concurrency): SKIP suppresses most fires
# while an action is still running, so far fewer actions start than under
# ALLOW_ALL — a signal robust to the small fire-time TOCTOU race (D22).
_overlap = {"started": 0}


@activity.defn
async def overlap_record() -> None:
    _overlap["started"] += 1


@workflow.defn
class OverlapAction:
    @workflow.run
    async def run(self, seconds: float) -> None:
        await workflow.execute_activity(
            overlap_record, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.sleep(seconds)


def _overlap_schedule(
    overlap: ScheduleOverlapPolicy, *, action_id: str, seconds: float = 2.0
) -> Schedule:
    return Schedule(
        action=ScheduleActionStartWorkflow(
            OverlapAction.run, seconds, id=action_id, task_queue=TASK_QUEUE
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]),
        policy=SchedulePolicy(overlap=overlap),
    )


async def _action_statuses(
    client: Client,
) -> "list[Optional[WorkflowExecutionStatus]]":
    rows = await client._dbos_client.list_workflows_async(name="wf:OverlapAction")
    statuses: "list[Optional[WorkflowExecutionStatus]]" = []
    for row in rows:
        desc = await client.get_workflow_handle(row.workflow_id).describe()
        statuses.append(desc.status)
    return statuses


def _fast_scheduler_config() -> DBOSConfig:
    config = default_config()
    # Make the dynamic scheduler poll quickly so automatic cron fires happen
    # within the test timeout.
    config["scheduler_polling_interval_sec"] = 0.1
    return config


@asynccontextmanager
async def _env(config: Optional[DBOSConfig] = None) -> AsyncIterator[Client]:
    worker = Worker(
        config or default_config(),
        task_queue=TASK_QUEUE,
        workflows=[ScheduledGreeter, OverlapAction],
        activities=[greet, overlap_record],
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
                result: str = await handle.result()
                return result
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


# --- overlap policies (DEVIATIONS D22) --------------------------------------


async def test_overlap_skip_suppresses_runs() -> None:
    # SKIP: while one ~3s action is running, the ~1/s occurrences are dropped,
    # so only a couple of actions start over the window (one extra tolerated for
    # the fire-time TOCTOU race, D22).
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-skip",
            _overlap_schedule(
                ScheduleOverlapPolicy.SKIP, action_id="ov-skip-wf", seconds=3.0
            ),
        )
        await asyncio.sleep(6.5)
        await client.get_schedule_handle("ov-skip").delete()
    assert _overlap["started"] <= 3


async def test_overlap_allow_all_runs_concurrently() -> None:
    # ALLOW_ALL: every ~1/s occurrence starts even while prior ~3s actions run,
    # so many more start than under SKIP (proving SKIP actually suppresses).
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-all",
            _overlap_schedule(
                ScheduleOverlapPolicy.ALLOW_ALL, action_id="ov-all-wf", seconds=3.0
            ),
        )
        await asyncio.sleep(6.5)
        await client.get_schedule_handle("ov-all").delete()
    assert _overlap["started"] >= 5


async def test_overlap_cancel_other_cancels_running() -> None:
    # CANCEL_OTHER: each new occurrence cooperatively cancels the still-running
    # prior action, so at least one ends CANCELED.
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-cancel",
            _overlap_schedule(
                ScheduleOverlapPolicy.CANCEL_OTHER, action_id="ov-cancel-wf"
            ),
        )
        await asyncio.sleep(5)
        statuses = await _action_statuses(client)
        await client.get_schedule_handle("ov-cancel").delete()
    assert WorkflowExecutionStatus.CANCELED in statuses


async def test_overlap_terminate_other_terminates_running() -> None:
    # TERMINATE_OTHER: each new occurrence forcefully terminates the prior
    # running action, so at least one ends TERMINATED.
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-term",
            _overlap_schedule(
                ScheduleOverlapPolicy.TERMINATE_OTHER, action_id="ov-term-wf"
            ),
        )
        await asyncio.sleep(5)
        statuses = await _action_statuses(client)
        await client.get_schedule_handle("ov-term").delete()
    assert WorkflowExecutionStatus.TERMINATED in statuses


async def test_buffer_overlap_rejected() -> None:
    async with _env() as client:
        with pytest.raises(NotImplementedError):
            await client.create_schedule(
                "ov-buffer",
                _overlap_schedule(
                    ScheduleOverlapPolicy.BUFFER_ONE, action_id="ov-buffer-wf"
                ),
            )
