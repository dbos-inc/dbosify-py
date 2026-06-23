"""Subprocess worker for cross-queue activity *cancellation* tests.
The activity runs on a different worker than the workflow, so
cancellation must reach it cross-process: the interpreter sets a checkpointed
cancel event, the activity's attempt step polls it on the other worker and
delivers an ``asyncio.CancelledError`` into the (async) activity.

  activity                an activities-only Worker hosting an activity that
                          records ``started`` then waits; on cancellation it
                          records ``cancelled`` in its ``except`` and re-raises.
  workflow start  <wf_id> a workflows-only Worker whose workflow starts the
                          activity cross-queue, cancels its handle, and prints
                          ``RESULT <text>``.
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbos import DBOS

from dbosify import activity, exceptions, workflow
from dbosify.worker import Worker
from dbosify.workflow import ActivityCancellationType
from tests.dbconfig import connect_client, default_config

ACTIVITY_TASK_QUEUE = "xq-cancel-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-cancel-workflow-tq"


@activity.defn(name="cancellable-activity")
async def cancellable_activity() -> str:
    path = os.environ["DBOSIFY_TEST_EFFECTS"]
    with open(path, "a") as f:
        f.write("started\n")
    print("ACTIVITY_STARTED", flush=True)
    try:
        await asyncio.sleep(60)
        return "done"
    except asyncio.CancelledError:
        with open(path, "a") as f:
            f.write("cancelled\n")
        # Announce cleanup so a TRY_CANCEL test (whose workflow resolves without
        # waiting for this worker) can synchronize before tearing it down.
        print("ACTIVITY_CANCELLED", flush=True)
        raise


@workflow.defn(name="cancel-cross-queue-workflow")
class CancelCrossQueueWorkflow:
    @workflow.run
    async def run(self, cancel_type: str, cancel_when: str = "delayed") -> str:
        handle = workflow.start_activity(
            cancellable_activity,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
            cancellation_type=(
                ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
                if cancel_type == "wait"
                else ActivityCancellationType.TRY_CANCEL
            ),
        )
        if cancel_when == "delayed":
            # Give the activity time to start on its worker, then cancel it.
            await workflow.sleep(3)
        # "immediate": cancel before dispatch commits. TRY_CANCEL retires the
        # never-dispatched activity; WAIT_CANCELLATION_COMPLETED signals across.
        handle.cancel()
        try:
            await handle
            return "completed"
        except (
            asyncio.CancelledError,
            exceptions.CancelledError,
            exceptions.ActivityError,
        ):
            return "activity-cancelled"


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[cancellable_activity],
    ):
        for _ in range(500):
            if await DBOS.retrieve_queue_async(ACTIVITY_TASK_QUEUE) is not None:
                break
            await asyncio.sleep(0.02)
        print("ACTIVITY_WORKER_READY", flush=True)
        await asyncio.Event().wait()


async def run_workflow_worker(workflow_id: str) -> None:
    async with Worker(
        default_config(),
        task_queue=WORKFLOW_TASK_QUEUE,
        workflows=[CancelCrossQueueWorkflow],
    ):
        client = await connect_client()
        try:
            handle = await client.start_workflow(
                CancelCrossQueueWorkflow.run,
                args=[
                    os.environ.get("DBOSIFY_TEST_CANCEL_TYPE", "try"),
                    os.environ.get("DBOSIFY_TEST_CANCEL_WHEN", "delayed"),
                ],
                id=workflow_id,
                task_queue=WORKFLOW_TASK_QUEUE,
            )
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
        asyncio.run(run_workflow_worker(sys.argv[3]))
    else:  # pragma: no cover - misuse
        raise SystemExit(f"unknown role {role!r}")


if __name__ == "__main__":
    main()
