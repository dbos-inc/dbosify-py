"""Subprocess worker for the cross-queue orphan test: a workflow that
continues-as-new with a fire-and-forget cross-queue activity still in flight.
The close path must cancel that activity (cross-process) rather than leave its
``__temporal_activity`` workflow running to completion on its worker.

  activity                an activities-only Worker hosting a slow, cancellable
                          activity (records ``cancelled`` on cancel, ``completed``
                          if it runs to the end).
  workflow start  <wf_id> a workflows-only Worker whose workflow starts the
                          activity fire-and-forget, then continues-as-new.
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbos import DBOS

from dbosify import activity, workflow
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

ACTIVITY_TASK_QUEUE = "xq-orphan-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-orphan-workflow-tq"


@activity.defn(name="slow-cancellable")
async def slow_cancellable() -> str:
    path = os.environ["DBOSIFY_TEST_EFFECTS"]
    with open(path, "a") as f:
        f.write("started\n")
    print("ACTIVITY_STARTED", flush=True)
    try:
        await asyncio.sleep(60)
    except asyncio.CancelledError:
        with open(path, "a") as f:
            f.write("cancelled\n")
        print("ACTIVITY_CANCELLED", flush=True)
        raise
    with open(path, "a") as f:
        f.write("completed\n")
    print("ACTIVITY_COMPLETED", flush=True)
    return "done"


@workflow.defn(name="orphan-can-workflow")
class OrphanCanWorkflow:
    @workflow.run
    async def run(self, iteration: int) -> str:
        if iteration == 0:
            # Fire-and-forget: start the cross-queue activity but never await it.
            workflow.start_activity(
                slow_cancellable,
                task_queue=ACTIVITY_TASK_QUEUE,
                start_to_close_timeout=timedelta(seconds=120),
            )
            await workflow.sleep(3)  # let it start on its worker
            workflow.continue_as_new(1)  # CAN with the activity in flight
        return "done"


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[slow_cancellable],
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
        workflows=[OrphanCanWorkflow],
    ):
        client = await connect_client()
        try:
            handle = await client.start_workflow(
                OrphanCanWorkflow.run,
                0,
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
