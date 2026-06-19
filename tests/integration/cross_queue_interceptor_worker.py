"""Subprocess workers for the cross-queue activity *interceptor* test
(DESIGN §6.1.2 + §6.8). The activity runs on a different worker process than the
workflow that calls it, and that activity worker carries its own
``Worker(interceptors=[...])`` — so this proves activity interceptors fire on the
queued path, applied by whichever worker actually runs the activity.

  activity                 activities-only Worker on the activity queue, with an
                           inbound interceptor that transforms the result. Runs
                           until killed.
  workflow <wf_id>         workflows-only Worker that starts the workflow and
                           prints ``RESULT <text>``.

Cooperating workers share a DBOS application version (the Worker pins a stable
default, so the two roles agree without env setup); each role gets a distinct
``DBOS__VMID``.
"""

import asyncio
import sys
from datetime import timedelta
from typing import Any

from dbos import DBOS, DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Worker,
)
from tests.dbconfig import default_config, system_database_url

ACTIVITY_TASK_QUEUE = "xq-ic-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-ic-workflow-tq"


@activity.defn(name="xq-ic-say-hello")
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn(name="xq-ic-workflow")
class CrossQueueWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            say_hello,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
        )
        return result


class _TransformInbound(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        result = await super().execute_activity(input)
        return f"xq-intercepted({result})"


class _TransformInterceptor(Interceptor):
    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _TransformInbound(next)


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[say_hello],
        interceptors=[_TransformInterceptor()],
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
        workflows=[CrossQueueWorkflow],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            handle = await client.start_workflow(
                CrossQueueWorkflow.run,
                "Temporal",
                id=workflow_id,
                task_queue=WORKFLOW_TASK_QUEUE,
            )
            print("RESULT " + await handle.result(), flush=True)
        finally:
            dbos_client.destroy()


def main() -> None:
    role = sys.argv[1]
    if role == "activity":
        asyncio.run(run_activity_worker())
    elif role == "workflow":
        asyncio.run(run_workflow_worker(sys.argv[2]))
    else:  # pragma: no cover - misuse
        raise SystemExit(f"unknown role {role!r}")


if __name__ == "__main__":
    main()
