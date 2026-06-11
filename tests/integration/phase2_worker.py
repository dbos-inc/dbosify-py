"""Subprocess worker for Phase 2 cancellation recovery tests.

Run as: python phase2_worker.py <start|resume> <workflow_id> <effects_path>

The workflow parks forever; on cancellation its unwind runs a cleanup
activity and then parks again awaiting a `go` signal — creating the kill
window *mid-unwind, after the cleanup checkpoint*.
"""

import asyncio
import json
import sys
from datetime import timedelta

from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client, WorkflowFailureError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "phase2-recovery-tq"


@activity.defn
async def record_cleanup(path: str) -> None:
    print("CLEANUP_ACTIVITY_EXECUTED", flush=True)
    with open(path, "a") as f:
        f.write("cleanup\n")


@workflow.defn
class CleanupHoldWorkflow:
    def __init__(self) -> None:
        self.proceed = False

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.run
    async def run(self, path: str) -> None:
        try:
            await workflow.wait_condition(lambda: False)
        finally:
            await workflow.execute_activity(
                record_cleanup, path, start_to_close_timeout=timedelta(seconds=10)
            )
            print("CLEANUP_DONE", flush=True)
            # Hold the unwind open so the test can SIGKILL mid-cancellation.
            await workflow.wait_condition(lambda: self.proceed)


async def main() -> None:
    mode, workflow_id, effects_path = sys.argv[1], sys.argv[2], sys.argv[3]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[CleanupHoldWorkflow],
        activities=[record_cleanup],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            if mode == "start":
                handle = await client.start_workflow(
                    CleanupHoldWorkflow.run,
                    effects_path,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
            else:
                assert mode == "resume"
                handle = client.get_workflow_handle(workflow_id)
            print("STARTED", flush=True)
            try:
                await handle.result()
                outcome = {"result": "completed"}
            except WorkflowFailureError as err:
                outcome = {"cause": type(err.cause).__name__}
            status = (await handle.describe()).status
            assert status is not None
            print(
                "RESULT " + json.dumps({**outcome, "status": status.name}), flush=True
            )
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
