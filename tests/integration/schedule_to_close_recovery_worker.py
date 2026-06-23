"""Subprocess worker proving a schedule-to-close timeout is durable across a crash.

Run as: python schedule_to_close_recovery_worker.py <start|resume> <workflow_id>

The activity has only ``schedule_to_close`` set and hangs forever; the budget times
it out with SCHEDULE_TO_CLOSE (before the fix this hung forever). The parent records
the outcome and parks. The test SIGKILLs the parked run and resumes in a fresh
process, where recovery replays run(): the activity's SCHEDULE_TO_CLOSE envelope is
replayed from its checkpoint (the activity is NOT re-run and re-timed), and the
released run returns the same outcome — proving the timeout decision was
checkpointed, not re-derived by luck.
"""

import asyncio
import sys
from datetime import timedelta

from dbosify import activity, workflow
from dbosify.common import RetryPolicy
from dbosify.exceptions import ActivityError, TimeoutError
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

TASK_QUEUE = "stc-recovery-tq"


@activity.defn
async def hang_forever() -> None:
    await asyncio.Event().wait()


@workflow.defn(name="ScheduleToCloseRecoveryWorkflow")
class ScheduleToCloseRecoveryWorkflow:
    def __init__(self) -> None:
        self.go = False
        self.outcome = "<unset>"

    @workflow.signal
    def release(self) -> None:
        self.go = True

    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.execute_activity(
                hang_forever,
                schedule_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            self.outcome = "no-timeout"
        except ActivityError as err:
            if isinstance(err.cause, TimeoutError):
                tt = err.cause.type
                self.outcome = "timed-out:" + (tt.name if tt is not None else "UNKNOWN")
            else:
                self.outcome = "other:" + type(err.cause).__name__
        # The timeout outcome is now derived from the activity's checkpoint; park
        # so the test can SIGKILL with everything needed to reconstruct it.
        print("PARKED", flush=True)
        await workflow.wait_condition(lambda: self.go)
        return self.outcome


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[ScheduleToCloseRecoveryWorkflow],
        activities=[hang_forever],
    ):
        client = await connect_client()
        try:
            if action == "start":
                handle = await client.start_workflow(
                    ScheduleToCloseRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal(ScheduleToCloseRecoveryWorkflow.release)
                print("RESULT " + await handle.result(), flush=True)
        finally:
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
