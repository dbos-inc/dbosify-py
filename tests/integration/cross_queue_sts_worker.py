"""Subprocess worker for the cross-queue schedule_to_start test (§6.1.2).
The test starts the workflow worker first (enqueueing the activity) and
delays the activity worker, so the activity sits in the queue past
``schedule_to_start_timeout`` and the activity workflow fails it before running.

  activity                an activities-only Worker (started late by the test).
  workflow start  <wf_id> a workflows-only Worker; the workflow reports the
                          activity's terminal outcome as ``RESULT <text>``.
"""

import asyncio
import sys
from datetime import timedelta

from dbos import DBOS

from dbosify import activity, exceptions, workflow
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

ACTIVITY_TASK_QUEUE = "xq-sts-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-sts-workflow-tq"


@activity.defn(name="quick-say-hello")
async def quick_say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn(name="sts-cross-queue-workflow")
class StsCrossQueueWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        try:
            result: str = await workflow.execute_activity(
                quick_say_hello,
                name,
                task_queue=ACTIVITY_TASK_QUEUE,
                schedule_to_start_timeout=timedelta(seconds=2),
                start_to_close_timeout=timedelta(seconds=30),
            )
            return result
        except exceptions.ActivityError as err:
            cause = err.__cause__
            if isinstance(cause, exceptions.TimeoutError) and cause.type is not None:
                return f"timeout:{cause.type.name}"
            return f"activity-error:{type(cause).__name__}"


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[quick_say_hello],
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
        workflows=[StsCrossQueueWorkflow],
    ):
        client = await connect_client()
        try:
            handle = await client.start_workflow(
                StsCrossQueueWorkflow.run,
                "Temporal",
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
