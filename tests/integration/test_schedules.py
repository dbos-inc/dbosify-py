"""Schedules (DESIGN §6.7): create/describe/list/update/pause/trigger/backfill/
delete against real Postgres, plus an automatic cron fire and persistence
across a worker restart.
"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional

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
from tests.harness import retry_until_success_async

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


@workflow.defn
class BlockingAction:
    """Records its start, then runs forever until released by signal — lets a
    test hold one action 'running' across an unbounded number of fires."""

    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            overlap_record, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.wait_condition(lambda: self._released)


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


async def _wait_for_action_status(
    client: Client, want: WorkflowExecutionStatus
) -> None:
    """Poll until some OverlapAction workflow reaches ``want`` (raises until)."""
    rows = await client._dbos_client.list_workflows_async(name="wf:OverlapAction")
    for row in rows:
        desc = await client.get_workflow_handle(row.workflow_id).describe()
        if desc.status == want:
            return
    raise AssertionError(f"no OverlapAction workflow is {want!r} yet")


async def _wait_for_started(at_least: int) -> None:
    """Poll until at least ``at_least`` actions have started (raises until)."""
    if _overlap["started"] < at_least:
        raise AssertionError(f"only {_overlap['started']} actions started")


async def _success_count(client: Client, name: str) -> int:
    rows = await client._dbos_client.list_workflows_async(name=name)
    return sum(1 for row in rows if row.status == "SUCCESS")


async def _wait_for_fire_count(client: Client, at_least: int) -> None:
    """Poll until at least ``at_least`` schedule fires have completed (each fire
    runs the dispatcher even when it skips), proving the schedule is firing."""
    done = await _success_count(client, "__temporal_schedule_fire")
    if done < at_least:
        raise AssertionError(f"only {done} schedule fires so far")


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
        workflows=[ScheduledGreeter, OverlapAction, BlockingAction],
        activities=[greet, overlap_record],
    )
    async with worker:
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            yield await Client.connect(dbos_client)
        finally:
            dbos_client.destroy()


async def _wait_for_action(client: Client, *, name: str = "World") -> str:
    """Wait for a scheduled action workflow to complete and return its result."""

    async def completed() -> str:
        rows = await client._dbos_client.list_workflows_async(name=ACTION_WF_NAME)
        for row in rows:
            if row.status == "SUCCESS":
                handle = client.get_workflow_handle(row.workflow_id, result_type=str)
                result: str = await handle.result()
                return result
        raise AssertionError("scheduled action workflow has not completed yet")

    return await retry_until_success_async(completed)


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
        # The pause note is accepted but not persisted (D22); the creation note
        # is unchanged.
        assert desc.schedule.state.note == "a note"

        await handle.unpause()
        desc = await handle.describe()
        assert desc.schedule.state.paused is False
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


async def test_fires_on_schedule_with_right_inputs() -> None:
    # An every-1s schedule should fire repeatedly, on the 1-second cron grid,
    # each run receiving the scheduled argument. We collect several completed
    # actions and check (a) every result reflects the scheduled input and (b) the
    # occurrence times encoded in the action ids are consecutive whole seconds —
    # i.e. it fired on time, once per period, not in a burst or with drift.
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "sched-ontime",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    ScheduledGreeter.run,
                    "Timely",
                    id="ontime-wf",
                    task_queue=TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]
                ),
                # ALLOW_ALL so every occurrence fires (the default SKIP would drop
                # occurrences while an action is still running, hiding the grid).
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.ALLOW_ALL),
            ),
        )

        async def four_completed() -> "list[str]":
            rows = await client._dbos_client.list_workflows_async(name=ACTION_WF_NAME)
            # Scope to THIS schedule's action runs (id ``ontime-wf-<epoch>``).
            # The name query is database-global and `ScheduledGreeter` is shared
            # with other schedule tests (e.g. test_automatic_cron_fire's
            # ``auto-wf-*`` runs), whose runs can bleed in when the process-global
            # scheduler / DB-reset lifecycle overlaps across tests — otherwise a
            # stray "Hello, Auto!" run fails the assertion below.
            done = [
                r.workflow_id
                for r in rows
                if r.status == "SUCCESS" and r.workflow_id.startswith("ontime-wf-")
            ]
            if len(done) < 4:
                raise AssertionError(f"only {len(done)} fired so far")
            return done

        ids = await retry_until_success_async(four_completed)
        await client.get_schedule_handle("sched-ontime").delete()

        # Right inputs: every fire ran with the scheduled arg.
        for wid in ids:
            handle = client.get_workflow_handle(wid, result_type=str)
            assert await handle.result() == "Hello, Timely!"

        # On time: action ids are ``ontime-wf-<epoch>`` where epoch is the nominal
        # cron occurrence. Four consecutive 1s occurrences → a contiguous run of
        # whole-second epochs (span == count - 1).
        epochs = sorted({int(wid.rsplit("-", 1)[1]) for wid in ids})
        assert epochs[-1] - epochs[0] == len(epochs) - 1, epochs


async def test_pause_stops_firing() -> None:
    # Pause must actually stop the schedule from firing (not just flip the
    # describe status). After pausing, the fire count must stop growing.
    async with _env(_fast_scheduler_config()) as client:
        handle = await client.create_schedule(
            "sched-pausefire",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    ScheduledGreeter.run,
                    "P",
                    id="pausefire-wf",
                    task_queue=TASK_QUEUE,
                ),
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]
                ),
            ),
        )
        # Confirm it is actively firing, then pause.
        await retry_until_success_async(lambda: _wait_for_fire_count(client, 2))
        await handle.pause()

        # The fire count must settle: sample until two reads spaced over several
        # cron periods agree (a paused schedule produces no new fires; at most one
        # already-enqueued fire may still land). Polling for *stability* is how we
        # assert the absence of further fires without a magic sleep.
        async def settled() -> int:
            first = await _success_count(client, "__temporal_schedule_fire")
            await asyncio.sleep(2.0)  # > 2 cron periods
            second = await _success_count(client, "__temporal_schedule_fire")
            if second > first + 1:
                raise AssertionError(f"still firing while paused: {first} -> {second}")
            return second

        paused_count = await retry_until_success_async(
            settled, interval=0.0, max_attempts=3
        )

        # Unpause: firing must resume, so the count climbs past the paused level.
        await handle.unpause()
        await retry_until_success_async(
            lambda: _wait_for_fire_count(client, paused_count + 2)
        )
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
    # Robust SKIP: the first action *blocks forever* (until released), so it stays
    # running across an unbounded number of occurrences. We wait until many fires
    # have happened (each runs the dispatcher, which skips), then assert that
    # despite all those fires only the one blocked action ever started (≤2,
    # tolerating the documented fire-time TOCTOU race, D22). A broken SKIP would
    # have started one action *per fire* (~6). The huge margin (≤2 vs ≥6) is what
    # makes this reliable rather than timing-dependent.
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        handle = await client.create_schedule(
            "ov-skip",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    BlockingAction.run, id="ov-skip-wf", task_queue=TASK_QUEUE
                ),
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]
                ),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
        # ≥6 occurrences fired while the first action was blocked.
        await retry_until_success_async(lambda: _wait_for_fire_count(client, 6))
        assert 1 <= _overlap["started"] <= 2
        # Release the blocked action so the worker can shut down cleanly.
        await handle.delete()
        for row in await client._dbos_client.list_workflows_async(
            name="wf:BlockingAction"
        ):
            if row.status in ("PENDING", "ENQUEUED"):
                await client.get_workflow_handle(row.workflow_id).signal(
                    BlockingAction.release
                )


async def test_overlap_allow_all_runs_concurrently() -> None:
    # ALLOW_ALL: every ~1/s occurrence starts even while prior ~3s actions run,
    # so several pile up concurrently (proving SKIP above actually suppresses).
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-all",
            _overlap_schedule(
                ScheduleOverlapPolicy.ALLOW_ALL, action_id="ov-all-wf", seconds=3.0
            ),
        )
        await retry_until_success_async(lambda: _wait_for_started(3))
        await client.get_schedule_handle("ov-all").delete()


async def test_overlap_cancel_other_cancels_running() -> None:
    # CANCEL_OTHER: each new occurrence cooperatively cancels the still-running
    # prior action, so one ends CANCELED.
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-cancel",
            _overlap_schedule(
                ScheduleOverlapPolicy.CANCEL_OTHER, action_id="ov-cancel-wf"
            ),
        )
        await retry_until_success_async(
            lambda: _wait_for_action_status(client, WorkflowExecutionStatus.CANCELED)
        )
        await client.get_schedule_handle("ov-cancel").delete()


async def test_overlap_terminate_other_terminates_running() -> None:
    # TERMINATE_OTHER: each new occurrence forcefully terminates the prior
    # running action, so one ends TERMINATED.
    _overlap.update(started=0)
    async with _env(_fast_scheduler_config()) as client:
        await client.create_schedule(
            "ov-term",
            _overlap_schedule(
                ScheduleOverlapPolicy.TERMINATE_OTHER, action_id="ov-term-wf"
            ),
        )
        await retry_until_success_async(
            lambda: _wait_for_action_status(client, WorkflowExecutionStatus.TERMINATED)
        )
        await client.get_schedule_handle("ov-term").delete()


async def test_buffer_overlap_rejected() -> None:
    # BUFFER_* is unsupported as a schedule policy.
    async with _env() as client:
        with pytest.raises(NotImplementedError):
            await client.create_schedule(
                "ov-buffer",
                _overlap_schedule(
                    ScheduleOverlapPolicy.BUFFER_ONE, action_id="ov-buffer-wf"
                ),
            )


async def test_trigger_overlap_override_rejected_except_allow_all() -> None:
    # A per-call overlap override is only accepted as ALLOW_ALL (D22); any other
    # value raises rather than being silently ignored. ALLOW_ALL is accepted and
    # the action still fires (under the schedule's configured policy).
    async with _env() as client:
        handle = await client.create_schedule("ov-override", _interval_schedule())
        with pytest.raises(NotImplementedError):
            await handle.trigger(overlap=ScheduleOverlapPolicy.SKIP)
        await handle.trigger(overlap=ScheduleOverlapPolicy.ALLOW_ALL)
        assert await _wait_for_action(client) == "Hello, World!"
        await handle.delete()
