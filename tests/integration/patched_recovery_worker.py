"""Subprocess worker for workflow.patched() recovery.

Run as: python patched_recovery_worker.py <start|resume> <workflow_id>
with env PATCH_VERSION in {"v1", "v2"} selecting the deployed code.

The same workflow type is deployed in two shapes:

  v1 (old code): two activities — noop("pre"), noop("old") — then park.
  v2 (new code): noop("pre"), then `if workflow.patched("v2")` choosing
       noop("new") (newer path) or noop("old") (older path), then park.

Because a False patched() claims no checkpoint position, v2's older branch
records the EXACT same activity sequence (pre, old) as v1 — so a v1 run that
crashed mid-flight can replay cleanly under freshly-deployed v2 code, taking the
older path (no marker in history → patched() returns False). A fresh v2 run
records the marker and replays the newer path. The returned branch label is the
observable proof of which path replay took.
"""

import asyncio
import os
import sys
from datetime import timedelta

from dbosify import activity, workflow
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

TASK_QUEUE = "patched-recovery-tq"
PATCH_VERSION = os.environ.get("PATCH_VERSION", "v2")

_TIMEOUT = timedelta(seconds=10)


@activity.defn
async def noop(label: str) -> str:
    return label


@workflow.defn(name="PatchRecoveryWorkflow")
class PatchRecoveryWorkflow:
    def __init__(self) -> None:
        self.go = False

    @workflow.signal
    def release(self) -> None:
        self.go = True

    @workflow.run
    async def run(self) -> str:
        await workflow.execute_activity(
            noop, args=["pre"], start_to_close_timeout=_TIMEOUT
        )
        if PATCH_VERSION == "v1":
            # Old code: the patch did not exist yet — one fixed path.
            await workflow.execute_activity(
                noop, args=["old"], start_to_close_timeout=_TIMEOUT
            )
            branch = "old"
        else:
            # New code: the same call site, now branched on a patch.
            if workflow.patched("v2"):
                await workflow.execute_activity(
                    noop, args=["new"], start_to_close_timeout=_TIMEOUT
                )
                branch = "new"
            else:
                await workflow.execute_activity(
                    noop, args=["old"], start_to_close_timeout=_TIMEOUT
                )
                branch = "old"
        # The discriminating activity is committed; announce + park so the test
        # can SIGKILL with the branch checkpoint durably recorded.
        print("PARKED", flush=True)
        await workflow.wait_condition(lambda: self.go)
        return branch


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[PatchRecoveryWorkflow],
        activities=[noop],
    ):
        client = await connect_client()
        try:
            if action == "start":
                handle = await client.start_workflow(
                    PatchRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                # Park until released (it won't be until resume); the test
                # SIGKILLs this process once it sees PARKED.
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal(PatchRecoveryWorkflow.release)
                branch = await handle.result()
                print("RESULT " + branch, flush=True)
        finally:
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
