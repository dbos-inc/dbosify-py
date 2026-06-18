"""Subprocess worker proving an authoritative start-to-close timeout is durable
across a crash.

Run as: python swallow_timeout_recovery_worker.py <start|resume> <workflow_id>

The activity catches CancelledError and returns a value, yet the authoritative
start-to-close deadline times it out (the late return is discarded). The parent
catches ActivityError(TimeoutError), records the outcome, and parks. The test
SIGKILLs the parked run and resumes in a fresh process; recovery replays run(),
the activity's timeout envelope is replayed from its checkpoint (the activity is
NOT re-run), and the released run returns the same outcome — proving the timeout
decision was checkpointed, not re-derived by luck.
"""

import asyncio
import sys
from datetime import timedelta

from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import ActivityError, TimeoutError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "swallow-timeout-recovery-tq"


@activity.defn
async def swallow_cancel_and_return() -> str:
    try:
        while True:
            await asyncio.sleep(0.2)
            activity.heartbeat()
    except asyncio.CancelledError:
        return "swallowed-and-returned"


@workflow.defn(name="SwallowTimeoutRecoveryWorkflow")
class SwallowTimeoutRecoveryWorkflow:
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
                swallow_cancel_and_return,
                start_to_close_timeout=timedelta(seconds=2),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            self.outcome = "no-timeout"
        except ActivityError as err:
            if isinstance(err.cause, TimeoutError):
                timeout_type = err.cause.type
                self.outcome = "timed-out:" + (
                    timeout_type.name if timeout_type is not None else "UNKNOWN"
                )
            else:
                self.outcome = "other:" + type(err.cause).__name__
        # The timeout outcome is now derived from the activity's checkpoint;
        # park so the test can SIGKILL with everything needed to reconstruct it.
        print("PARKED", flush=True)
        await workflow.wait_condition(lambda: self.go)
        return self.outcome


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[SwallowTimeoutRecoveryWorkflow],
        activities=[swallow_cancel_and_return],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    SwallowTimeoutRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal(SwallowTimeoutRecoveryWorkflow.release)
                print("RESULT " + await handle.result(), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
