"""Subprocess worker for cross-queue (queued) activity versioning (DEVIATIONS
worker-versioning). Two roles run as separate processes, both on the same build id:

  activity            an activities-only Worker on the activity queue.
  workflow  <wf_id>   a workflows-only Worker that runs a workflow which calls
                      the activity cross-queue, then prints ``RESULT <text>``.

A cross-queue activity is enqueued in-workflow as a ``__temporal_activity``
workflow, so DBOS stamps it with the workflow worker's application_version
(= build id); the activity worker dequeues it only if its own build id matches.
Both roles run on build id ``$DBOSIFY_BUILD_ID``.
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbos import DBOS, DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

ACTIVITY_TASK_QUEUE = "version-xq-activity-tq"
WORKFLOW_TASK_QUEUE = "version-xq-workflow-tq"


@activity.defn(name="version-xq-hello")
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn(name="VersionCrossQueueWorkflow")
class VersionCrossQueueWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            say_hello,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
        )
        return result


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[say_hello],
        build_id=os.environ["DBOSIFY_BUILD_ID"],
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
        workflows=[VersionCrossQueueWorkflow],
        build_id=os.environ["DBOSIFY_BUILD_ID"],
    ):
        client = Client(DBOSClient(system_database_url=system_database_url()))
        result = await client.execute_workflow(
            VersionCrossQueueWorkflow.run,
            "Temporal",
            id=workflow_id,
            task_queue=WORKFLOW_TASK_QUEUE,
        )
        print("RESULT " + result, flush=True)


def main() -> None:
    role = sys.argv[1]
    if role == "activity":
        asyncio.run(run_activity_worker())
    else:
        assert role == "workflow"
        asyncio.run(run_workflow_worker(sys.argv[2]))


if __name__ == "__main__":
    main()
