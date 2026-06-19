"""Subprocess worker for Phase 1 recovery tests, written entirely against
the public API (Client.connect + Worker), the way a real Temporal worker
process is.

Run as: python phase1_worker.py <start|resume> <workflow_id>
"""

import asyncio
import json
import sys
from datetime import timedelta
from typing import List

from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "phase1-recovery-tq"


@activity.defn
async def record(step: str) -> str:
    print(f"ACTIVITY {step}", flush=True)
    return step


@workflow.defn
class TwoStageWorkflow:
    def __init__(self) -> None:
        self.proceed = False

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.run
    async def run(self) -> List[str]:
        stages = [
            await workflow.execute_activity(
                record, "one", start_to_close_timeout=timedelta(seconds=10)
            )
        ]
        print("STAGE_ONE_DONE", flush=True)
        # The kill window: stage one checkpointed, workflow parked.
        await workflow.wait_condition(lambda: self.proceed)
        stages.append(
            await workflow.execute_activity(
                record, "two", start_to_close_timeout=timedelta(seconds=10)
            )
        )
        return stages


async def main() -> None:
    mode, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TwoStageWorkflow],
        activities=[record],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            if mode == "start":
                handle = await client.start_workflow(
                    TwoStageWorkflow.run, id=workflow_id, task_queue=TASK_QUEUE
                )
            else:
                assert mode == "resume"
                handle = client.get_workflow_handle(workflow_id)
            print("STARTED", flush=True)
            result = await handle.result()
            print("RESULT " + json.dumps(result), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
