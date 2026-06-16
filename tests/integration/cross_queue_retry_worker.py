"""Subprocess worker for cross-queue activity *retry* tests (Phase 2, §6.1.2).

The retry loop lives on the activity worker (Design A): a queued activity that
fails retries on its own worker, with durable backoff, until success or the
policy gives up. Two roles run as separate processes:

  activity                an activities-only Worker hosting a flaky activity
                          that appends ``activity.info().attempt`` to the
                          effects file each run and (per env) fails or succeeds.
  workflow start  <wf_id> a workflows-only Worker that calls the activity
                          cross-queue under a RetryPolicy and prints
                          ``RESULT <text>`` on success or ``FAILED <cause>`` on
                          a terminal failure.

Env knobs (read by the activity, which runs on the activity worker):
  TDB_TEST_EFFECTS       path to append attempt numbers to (one per real run)
  TDB_TEST_SUCCEED_AT    attempt number at which the activity succeeds (default 1)
  TDB_TEST_NON_RETRYABLE "1" -> raise a non-retryable ApplicationError on attempt 1
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbos import DBOS, DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client, WorkflowFailureError
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import ApplicationError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

ACTIVITY_TASK_QUEUE = "xq-retry-activity-tq"
WORKFLOW_TASK_QUEUE = "xq-retry-workflow-tq"


@activity.defn(name="flaky-say-hello")
async def flaky_say_hello(name: str) -> str:
    attempt = activity.info().attempt
    with open(os.environ["TDB_TEST_EFFECTS"], "a") as f:
        f.write(f"{attempt}\n")
    print(f"ATTEMPT {attempt}", flush=True)
    if os.environ.get("TDB_TEST_NON_RETRYABLE") == "1":
        raise ApplicationError("boom (non-retryable)", type="Boom", non_retryable=True)
    if attempt < int(os.environ.get("TDB_TEST_SUCCEED_AT", "1")):
        raise ApplicationError(f"boom on attempt {attempt}", type="Boom")
    return f"Hello, {name}!"


@workflow.defn(name="retry-cross-queue-workflow")
class RetryCrossQueueWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        # Constant 2s backoff keeps the SIGKILL-mid-backoff window wide and
        # deterministic; maximum_attempts is high enough for the success cases.
        result: str = await workflow.execute_activity(
            flaky_say_hello,
            name,
            task_queue=ACTIVITY_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                backoff_coefficient=1.0,
                maximum_attempts=10,
            ),
        )
        return result


async def run_activity_worker() -> None:
    async with Worker(
        default_config(),
        task_queue=ACTIVITY_TASK_QUEUE,
        activities=[flaky_say_hello],
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
        workflows=[RetryCrossQueueWorkflow],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            handle = await client.start_workflow(
                RetryCrossQueueWorkflow.run,
                "Temporal",
                id=workflow_id,
                task_queue=WORKFLOW_TASK_QUEUE,
            )
            print("STARTED", flush=True)
            try:
                result = await handle.result()
                print("RESULT " + result, flush=True)
            except WorkflowFailureError as err:
                print("FAILED " + type(err.cause).__name__, flush=True)
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
