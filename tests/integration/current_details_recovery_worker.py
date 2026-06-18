"""Subprocess worker for workflow.set_current_details() recovery (audit item 3).

Run as: python current_details_recovery_worker.py <start|resume> <workflow_id>

Current details are in-memory workflow state — not a checkpoint. The proof they
survive a crash is that they are *reconstructed by replay*: run() derives the
details from a checkpointed activity result, sets them, then parks. The test
SIGKILLs the parked run and resumes in a fresh process; recovery replays run()
from the top, the activity result is replayed from its checkpoint, and
set_current_details() re-runs with the same value. The released run then returns
get_current_details(), which must equal the originally-set value.
"""

import asyncio
import sys
from datetime import timedelta

from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "current-details-recovery-tq"
_TIMEOUT = timedelta(seconds=10)


@activity.defn
async def make_label() -> str:
    return "details-from-activity"


@workflow.defn(name="DetailsRecoveryWorkflow")
class DetailsRecoveryWorkflow:
    def __init__(self) -> None:
        self.go = False

    @workflow.signal
    def release(self) -> None:
        self.go = True

    @workflow.run
    async def run(self) -> str:
        label = await workflow.execute_activity(
            make_label, start_to_close_timeout=_TIMEOUT
        )
        workflow.set_current_details(label)
        # Details are set and the activity checkpoint is durable; park so the
        # test can SIGKILL with everything needed to reconstruct on replay.
        print("PARKED", flush=True)
        await workflow.wait_condition(lambda: self.go)
        return workflow.get_current_details()


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[DetailsRecoveryWorkflow],
        activities=[make_label],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    DetailsRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal(DetailsRecoveryWorkflow.release)
                print("RESULT " + await handle.result(), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
