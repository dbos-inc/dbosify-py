"""Subprocess worker for Phase 3 recovery tests.

Run as: python phase3_worker.py <scenario>-<start|resume> <workflow_id> <effects_path>

Scenarios:
  chain   continue-as-new chain: each run records itself via an activity,
          sleeps, then continues as new; the kill lands mid-chain. Recovery
          must resume the in-flight run from its checkpoints and finish the
          chain — each run's activity exactly once, no twin runs (the next
          run id is deterministic, so a replayed enqueue re-attaches).
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

TASK_QUEUE = "phase3-recovery-tq"


@activity.defn
async def record_run(path: str, index: int) -> None:
    print(f"RUN_ACTIVITY {index}", flush=True)
    with open(path, "a") as f:
        f.write(f"run{index}\n")


@workflow.defn
class TimedChainWorkflow:
    @workflow.run
    async def run(self, path: str, index: int) -> str:
        await workflow.execute_activity(
            record_run, args=[path, index], start_to_close_timeout=timedelta(seconds=10)
        )
        print(f"CHAIN_RUN {index}", flush=True)
        if index == 4:
            return "chain-done"
        await workflow.sleep(0.5)
        workflow.continue_as_new(args=[path, index + 1])


async def main() -> None:
    mode, workflow_id, effects_path = sys.argv[1], sys.argv[2], sys.argv[3]
    scenario, _, action = mode.partition("-")
    assert scenario == "chain"
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TimedChainWorkflow],
        activities=[record_run],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    TimedChainWorkflow.run,
                    args=[effects_path, 0],
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
            print("STARTED", flush=True)
            try:
                result = await handle.result()
                outcome = {"result": result}
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
