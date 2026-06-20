"""Subprocess worker for recovery tests.

Run as: python continue_as_new_recovery_worker.py <scenario>-<start|resume> <workflow_id> <effects_path>

Scenarios:
  chain   continue-as-new chain: each run records itself via an activity,
          sleeps, then continues as new; the kill lands mid-chain. Recovery
          must resume the in-flight run from its checkpoints and finish the
          chain — each run's activity exactly once, no twin runs (the next
          run id is deterministic, so a replayed enqueue re-attaches).
  asyncact  async activity completion: the activity writes its task token to
          the effects file and raises complete-async; the kill lands while
          the activity is parked awaiting external completion. Recovery
          must re-park it from the checkpointed marker (without re-running
          the activity function); the test then completes it by token.
"""

import asyncio
import json
import sys
from datetime import timedelta

from dbosify import activity, workflow
from dbosify.client import WorkflowFailureError
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

TASK_QUEUE = "continue-as-new-recovery-tq"


@activity.defn
async def record_run(path: str, index: int) -> None:
    print(f"RUN_ACTIVITY {index}", flush=True)
    with open(path, "a") as f:
        f.write(f"run{index}\n")


@activity.defn
async def write_token(path: str) -> str:
    with open(path, "w") as f:
        f.write(activity.info().task_token.decode())
    print("TOKEN_WRITTEN", flush=True)
    activity.raise_complete_async()


@workflow.defn
class AsyncActWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        result: str = await workflow.execute_activity(
            write_token, path, start_to_close_timeout=timedelta(seconds=120)
        )
        return result


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
    run_refs = {"chain": TimedChainWorkflow.run, "asyncact": AsyncActWorkflow.run}
    run_ref = run_refs[scenario]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[TimedChainWorkflow, AsyncActWorkflow],
        activities=[record_run, write_token],
    ):
        client = await connect_client()
        try:
            if action == "start":
                start_args = (
                    [effects_path, 0] if scenario == "chain" else [effects_path]
                )
                handle = await client.start_workflow(
                    run_ref,
                    args=start_args,
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
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
