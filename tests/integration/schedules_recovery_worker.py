"""Subprocess worker for schedule recovery tests.

Run as: python schedules_recovery_worker.py <start|resume> <effects_path>

  start   Create an every-second schedule whose action records its (unique,
          deterministic) occurrence id via an activity, then sleeps ~2s and
          completes. Block until killed. The kill lands after an action has
          recorded its occurrence but before it finishes sleeping.
  resume  Just launch the worker. DBOS recovers the in-flight action from its
          checkpoints (the record activity is NOT re-executed), and the
          persisted schedule keeps firing. Wait until several distinct
          occurrences have recorded, then delete the schedule and report.
"""

import asyncio
import sys
from datetime import timedelta
from pathlib import Path

from dbos import DBOSClient, DBOSConfig

from temporal_dbos import activity, workflow
from temporal_dbos.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "schedules-recovery-tq"
SCHEDULE_ID = "recovery-sched"


@activity.defn
async def record_occurrence(path: str, occurrence_id: str) -> None:
    with open(path, "a") as f:
        f.write(occurrence_id + "\n")


@workflow.defn
class ScheduledRecoveryAction:
    @workflow.run
    async def run(self, path: str) -> None:
        occurrence_id = workflow.info().workflow_id
        await workflow.execute_activity(
            record_occurrence,
            args=[path, occurrence_id],
            start_to_close_timeout=timedelta(seconds=10),
        )
        print(f"ACTION_FIRED {occurrence_id}", flush=True)
        # Wide window for the kill to land mid-flight (after the record
        # checkpoint, before the run closes).
        await workflow.sleep(2)
        print(f"ACTION_DONE {occurrence_id}", flush=True)


def _config() -> DBOSConfig:
    config: DBOSConfig = default_config()
    config["scheduler_polling_interval_sec"] = 0.1
    return config


def _distinct_occurrences(path: str) -> int:
    text = Path(path).read_text() if Path(path).exists() else ""
    return len(set(text.splitlines()))


async def main() -> None:
    mode, effects_path = sys.argv[1], sys.argv[2]
    async with Worker(
        _config(),
        task_queue=TASK_QUEUE,
        workflows=[ScheduledRecoveryAction],
        activities=[record_occurrence],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            if mode == "start":
                await client.create_schedule(
                    SCHEDULE_ID,
                    Schedule(
                        action=ScheduleActionStartWorkflow(
                            ScheduledRecoveryAction.run,
                            effects_path,
                            id="recovery-action",
                            task_queue=TASK_QUEUE,
                        ),
                        spec=ScheduleSpec(
                            intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]
                        ),
                    ),
                )
                print("STARTED", flush=True)
                await asyncio.Event().wait()  # block until SIGKILLed
            else:
                assert mode == "resume"
                print("RESUMED", flush=True)
                while _distinct_occurrences(effects_path) < 3:
                    await asyncio.sleep(0.1)
                await client.get_schedule_handle(SCHEDULE_ID).delete()
                print("DONE", flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
