"""Subprocess worker for cross-queue (distributed) activity recovery tests
(DESIGN §6.1.2). Two roles run as separate processes so an activity genuinely
executes on a different worker than the workflow that calls it:

  activity                 an activities-only Worker on the activity queue,
                           hosting a deliberately slow activity (prints
                           ``ACTIVITY_RUNNING`` then sleeps then
                           ``ACTIVITY_DONE``, appending one line to the effects
                           file per real execution). Runs until killed.
  workflow start  <wf_id>  a workflows-only Worker that starts the workflow and
                           awaits it, printing ``RESULT <text>``.
  workflow resume <wf_id>  same, but re-attaches to an existing run (recovery).

Cooperating workers share a DBOS application version (DBOS scopes queue
dequeuing by it); the Worker pins a stable default, so the two roles agree
without env setup. Each role gets a distinct ``DBOS__VMID`` so recovery is
queue-scoped (a restarted worker recovers only its own queue's workflows).
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbos import DBOS

from dbosify import activity, workflow
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

ACTIVITY_TASK_QUEUE = "xq-recovery-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-recovery-workflow-tq"
ACTIVITY_SLEEP_SECONDS = 4.0


@activity.defn(name="slow-say-hello")
async def slow_say_hello(name: str) -> str:
    print("ACTIVITY_RUNNING", flush=True)
    # One line per real execution: the kill-the-activity-worker test re-runs
    # the activity (at-least-once), the kill-the-workflow-worker test must not.
    with open(os.environ["DBOSIFY_TEST_EFFECTS"], "a") as f:
        f.write("ran\n")
    await asyncio.sleep(ACTIVITY_SLEEP_SECONDS)
    print("ACTIVITY_DONE", flush=True)
    return f"Hello, {name}!"


@workflow.defn(name="slow-cross-queue-workflow")
class SlowCrossQueueWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            slow_say_hello,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
        )
        return result


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[slow_say_hello],
    ):
        for _ in range(500):
            if await DBOS.retrieve_queue_async(ACTIVITY_TASK_QUEUE) is not None:
                break
            await asyncio.sleep(0.02)
        print("ACTIVITY_WORKER_READY", flush=True)
        await asyncio.Event().wait()


async def run_workflow_worker(action: str, workflow_id: str) -> None:
    async with Worker(
        default_config(),
        task_queue=WORKFLOW_TASK_QUEUE,
        workflows=[SlowCrossQueueWorkflow],
    ):
        client = await connect_client()
        try:
            if action == "start":
                handle = await client.start_workflow(
                    SlowCrossQueueWorkflow.run,
                    "Temporal",
                    id=workflow_id,
                    task_queue=WORKFLOW_TASK_QUEUE,
                )
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
            print("STARTED", flush=True)
            result = await handle.result()
            print("RESULT " + result, flush=True)
        finally:
            await client.close()


def main() -> None:
    role = sys.argv[1]
    if role == "activity":
        asyncio.run(run_activity_worker())
    elif role == "workflow":
        action, workflow_id = sys.argv[2], sys.argv[3]
        asyncio.run(run_workflow_worker(action, workflow_id))
    else:  # pragma: no cover - misuse
        raise SystemExit(f"unknown role {role!r}")


if __name__ == "__main__":
    main()
