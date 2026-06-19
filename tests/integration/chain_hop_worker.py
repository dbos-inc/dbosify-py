"""Subprocess worker for chain-hop (cron / workflow-retry) recovery tests.

Run as: python chain_hop_worker.py <scenario>-<start|resume> <workflow_id> <effects_path>

Scenarios:
  cron    every-second cron chain: each run records its fire index via an
          activity (threaded through get_last_completion_result), sleeps,
          and returns. The kill lands mid-run, between the recorded
          activity and the run's close — i.e. before the chain hop exists.
          Recovery must resume the run from its checkpoints (activity not
          re-executed) and the hop must enqueue exactly one successor.
  retry   workflow retry chain: each attempt records itself, sleeps, and
          fails until attempt 3. The kill lands mid-attempt-2. Recovery
          must resume the attempt, replay its recorded activity, fail it,
          and start attempt 3 exactly once.
"""

import asyncio
import json
import sys
from datetime import timedelta

from dbos import DBOSClient

from dbosify import activity, workflow
from dbosify.client import Client
from dbosify.common import RetryPolicy
from dbosify.exceptions import ApplicationError
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "chain-hop-recovery-tq"


@activity.defn
async def record(path: str, label: str) -> None:
    print(f"RECORDED {label}", flush=True)
    with open(path, "a") as f:
        f.write(label + "\n")


@workflow.defn
class CronRecoveryWorkflow:
    @workflow.run
    async def run(self, path: str) -> int:
        last = workflow.get_last_completion_result()
        count = (last + 1) if workflow.has_last_completion_result() else 0
        await workflow.execute_activity(
            record,
            args=[path, f"fire{count}"],
            start_to_close_timeout=timedelta(seconds=10),
        )
        print(f"CRON_RUN {count}", flush=True)
        await workflow.sleep(0.5)
        return count


@workflow.defn
class RetryRecoveryWorkflow:
    @workflow.run
    async def run(self, path: str) -> str:
        attempt = workflow.info().attempt
        await workflow.execute_activity(
            record,
            args=[path, f"attempt{attempt}"],
            start_to_close_timeout=timedelta(seconds=10),
        )
        print(f"RETRY_ATTEMPT {attempt}", flush=True)
        await workflow.sleep(0.5)
        if attempt < 3:
            raise ApplicationError(f"attempt {attempt} fails")
        return f"succeeded on attempt {attempt}"


async def _wait_for_chain_index(client: Client, workflow_id: str, index: int) -> None:
    while True:
        current = await client._current_run(workflow_id)
        if current is not None and current[0] >= index:
            return
        await asyncio.sleep(0.1)


async def main() -> None:
    mode, workflow_id, effects_path = sys.argv[1], sys.argv[2], sys.argv[3]
    scenario, _, action = mode.partition("-")
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[CronRecoveryWorkflow, RetryRecoveryWorkflow],
        activities=[record],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            if action == "start":
                if scenario == "cron":
                    await client.start_workflow(
                        CronRecoveryWorkflow.run,
                        effects_path,
                        id=workflow_id,
                        task_queue=TASK_QUEUE,
                        cron_schedule="* * * * * *",
                    )
                else:
                    assert scenario == "retry"
                    await client.start_workflow(
                        RetryRecoveryWorkflow.run,
                        effects_path,
                        id=workflow_id,
                        task_queue=TASK_QUEUE,
                        retry_policy=RetryPolicy(
                            initial_interval=timedelta(milliseconds=500),
                            maximum_attempts=5,
                        ),
                    )
            print("STARTED", flush=True)
            if scenario == "cron":
                # Let the chain reach run index 3, then stop it and report
                # the first three runs' results.
                await _wait_for_chain_index(client, workflow_id, 3)
                results = []
                for index in range(3):
                    run_id = workflow_id if index == 0 else f"{workflow_id}--r{index}"
                    results.append(
                        await client.get_workflow_handle(
                            workflow_id, run_id=run_id
                        ).result(follow_runs=False)
                    )
                await client.get_workflow_handle(workflow_id).terminate()
                print("RESULT " + json.dumps(results), flush=True)
            else:
                result = await client.get_workflow_handle(workflow_id).result()
                print("RESULT " + json.dumps(result), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
