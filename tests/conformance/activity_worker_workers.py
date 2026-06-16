"""Two-worker helper for the ``activity_worker/`` conformance test.

The samples-python ``activity_worker/`` corpus is a single cross-language
sample: a Go workflow invoking a Python activity over a Temporal server. It
cannot run as written against temporal-dbos (no server, no Go worker;
``execute_workflow("say-hello-workflow", ...)`` targets a workflow registered
only in Go). This helper discharges the Phase-3 exit gate by proving the
capability the directory demonstrates — an **activity-only** Python worker
reachable from a workflow on a **different task queue** (the cross-queue /
distributed activity path, DESIGN §6.1.2).

The Go ``say-hello-workflow`` is substituted by ``SayHelloWorkflow`` (the
documented migration delta); the activity is the sample's ``say_hello_activity``
verbatim (samples-python ``activity_worker/activity_worker.py``).

Run as: ``python activity_worker_workers.py <activity|workflow>``
  activity  an activities-only Worker on the activity queue (runs until killed)
  workflow  a workflows-only Worker that runs SayHelloWorkflow to completion and
            prints ``RESULT <text>``
"""

import asyncio
import sys
from datetime import timedelta

from dbos import DBOS, DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

ACTIVITY_TASK_QUEUE = "say-hello-task-queue"
WORKFLOW_TASK_QUEUE = "say-hello-workflow-tq"
WORKFLOW_ID = "say-hello-wf"


@activity.defn(name="say-hello-activity")
async def say_hello_activity(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn(name="say-hello-workflow")
class SayHelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        # task_queue differs from this workflow's own queue -> the activity
        # runs on the activities-only worker via the cross-queue path.
        result: str = await workflow.execute_activity(
            say_hello_activity,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=30),
        )
        return result


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[say_hello_activity],
    ):
        # The Worker context manager returns before run() has persisted the
        # queue; wait until it exists so the workflow worker can enqueue to it.
        for _ in range(500):
            if await DBOS.retrieve_queue_async(ACTIVITY_TASK_QUEUE) is not None:
                break
            await asyncio.sleep(0.02)
        print("ACTIVITY_WORKER_READY", flush=True)
        await asyncio.Event().wait()  # run until the test terminates us


async def run_workflow_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=WORKFLOW_TASK_QUEUE,
        workflows=[SayHelloWorkflow],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                SayHelloWorkflow.run,
                "Temporal",
                id=WORKFLOW_ID,
                task_queue=WORKFLOW_TASK_QUEUE,
            )
            result = await handle.result()
            print("RESULT " + result, flush=True)
        finally:
            dbos_client.destroy()


def main() -> None:
    role = sys.argv[1]
    if role == "activity":
        asyncio.run(run_activity_worker())
    elif role == "workflow":
        asyncio.run(run_workflow_worker())
    else:  # pragma: no cover - misuse
        raise SystemExit(f"unknown role {role!r}")


if __name__ == "__main__":
    main()
