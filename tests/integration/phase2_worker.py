"""Subprocess worker for Phase 2 recovery tests.

Run as: python phase2_worker.py <scenario>-<start|resume> <workflow_id> <effects_path>

Scenarios:
  cancel  cancellation unwind: parks forever; on cancel the unwind runs a
          cleanup activity then parks awaiting `go` — the kill window is
          mid-unwind, after the cleanup checkpoint.
  child   child re-attach: parent starts a slow recording child and awaits
          it — the kill window is after the child started, before it
          completed; recovery must re-attach, not spawn a twin.
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


@activity.defn
async def record_child_work(path: str) -> str:
    print("CHILD_ACTIVITY_EXECUTED", flush=True)
    with open(path, "a") as f:
        f.write("child-work\n")
    return "done"


@workflow.defn
class SlowChild:
    @workflow.run
    async def run(self, path: str) -> str:
        print("CHILD_STARTED", flush=True)
        await workflow.sleep(3.0)
        result: str = await workflow.execute_activity(
            record_child_work, path, start_to_close_timeout=timedelta(seconds=10)
        )
        return result


@workflow.defn
class ChildParent:
    @workflow.run
    async def run(self, path: str) -> str:
        result: str = await workflow.execute_child_workflow(
            SlowChild.run, path, id="reattach-child"
        )
        return f"parent saw: {result}"


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
    scenario, _, action = mode.partition("-")
    run_refs = {"cancel": CleanupHoldWorkflow.run, "child": ChildParent.run}
    run_ref = run_refs[scenario]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[CleanupHoldWorkflow, ChildParent, SlowChild],
        activities=[record_cleanup, record_child_work],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    run_ref,
                    effects_path,
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
