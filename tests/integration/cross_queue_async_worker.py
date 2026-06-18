"""Subprocess worker for cross-queue async-completion tests
(raise_complete_async on the queued path, Phase 4, §6.1.2).

The activity records its ``info().task_token`` (which carries the activity
workflow id) and parks via ``raise_complete_async()``; the ``__temporal_activity``
workflow then waits on its completion topic. The test process uses the token to
complete the activity externally, and the workflow returns the result.

  activity                an activities-only Worker hosting the parking activity.
  workflow start  <wf_id> a workflows-only Worker that runs the workflow and
                          prints ``RESULT <text>``.
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbos import DBOS, DBOSClient

from temporal_dbos import activity, exceptions, workflow
from temporal_dbos.client import Client
from temporal_dbos.worker import Worker
from temporal_dbos.workflow import ActivityCancellationType
from tests.dbconfig import default_config, system_database_url

ACTIVITY_TASK_QUEUE = "xq-async-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-async-workflow-tq"


@activity.defn(name="async-say-hello")
async def async_say_hello(name: str) -> str:
    # Hand the task token to an external completer (when one is expected), then
    # complete async — the workflow parks until externally completed/cancelled.
    token_path = os.environ.get("TDB_TEST_TOKEN")
    if token_path:
        with open(token_path, "wb") as f:
            f.write(activity.info().task_token)
    print("ACTIVITY_PARKED", flush=True)
    activity.raise_complete_async()


@workflow.defn(name="async-cross-queue-workflow")
class AsyncCrossQueueWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        result: str = await workflow.execute_activity(
            async_say_hello,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=120),
        )
        return result


@workflow.defn(name="cancel-parked-workflow")
class CancelParkedWorkflow:
    """Cancels a queued activity that has async-parked. Distinguishes a real
    cancellation (the marker woke the parked recv) from the start-to-close
    timeout that would occur if the cancel never reached the parked activity."""

    @workflow.run
    async def run(self, name: str) -> str:
        handle = workflow.start_activity(
            async_say_hello,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=120),
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        await workflow.sleep(3)  # let the activity park
        handle.cancel()
        try:
            await handle
            return "completed"
        except (asyncio.CancelledError, exceptions.CancelledError):
            return "activity-cancelled"
        except exceptions.ActivityError as err:
            cause = err.__cause__
            if isinstance(cause, exceptions.CancelledError):
                return "activity-cancelled"
            if isinstance(cause, exceptions.TimeoutError) and cause.type is not None:
                return f"timeout:{cause.type.name}"
            return f"error:{type(cause).__name__}"


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[async_say_hello],
    ):
        for _ in range(500):
            if await DBOS.retrieve_queue_async(ACTIVITY_TASK_QUEUE) is not None:
                break
            await asyncio.sleep(0.02)
        print("ACTIVITY_WORKER_READY", flush=True)
        await asyncio.Event().wait()


async def run_workflow_worker(workflow_id: str) -> None:
    run_ref = (
        CancelParkedWorkflow.run
        if os.environ.get("TDB_TEST_WF") == "cancel"
        else AsyncCrossQueueWorkflow.run
    )
    async with Worker(
        default_config(),
        task_queue=WORKFLOW_TASK_QUEUE,
        workflows=[AsyncCrossQueueWorkflow, CancelParkedWorkflow],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            handle = await client.start_workflow(
                run_ref,
                "Temporal",
                id=workflow_id,
                task_queue=WORKFLOW_TASK_QUEUE,
            )
            print("STARTED", flush=True)
            result = await handle.result()
            print("RESULT " + result, flush=True)
        finally:
            dbos_client.destroy()


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
