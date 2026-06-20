"""Subprocess worker for cross-version PINNED-recovery (DEVIATIONS worker-versioning).

Run as: python version_recovery_worker.py <start|idle|resume> <workflow_id>
with env DBOSIFY_BUILD_ID selecting the worker's build id (= DBOS application_version).

DBOS scopes recovery to application_version, so a workflow stamped with build id
v1 is recovered only by v1 workers. This script demonstrates that end-to-end:

  start  (v1): start the workflow (stamped v1); it parks on a release signal.
  idle   (v2): launch a *different-version* worker, send the release signal, wait,
               then print the workflow's status. A v2 worker must NOT recover the
               v1 workflow, so the buffered release is never processed and the
               status stays RUNNING (not COMPLETED).
  resume (v1): launch a same-version worker; it recovers the workflow, processes
               the buffered release, and completes — printing the result.
"""

import asyncio
import os
import sys

from dbosify import workflow
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

TASK_QUEUE = "version-recovery-tq"
BUILD_ID = os.environ["DBOSIFY_BUILD_ID"]


@workflow.defn(name="VersionPinnedWorkflow")
class VersionPinnedWorkflow:
    def __init__(self) -> None:
        self.go = False

    @workflow.signal
    def release(self) -> None:
        self.go = True

    @workflow.run
    async def run(self) -> str:
        print("PARKED", flush=True)
        await workflow.wait_condition(lambda: self.go)
        return "released"


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[VersionPinnedWorkflow],
        build_id=BUILD_ID,
    ):
        client = await connect_client()
        if action == "start":
            handle = await client.start_workflow(
                VersionPinnedWorkflow.run, id=workflow_id, task_queue=TASK_QUEUE
            )
            await handle.result()
        elif action == "idle":
            # A different-version worker: send the release, then confirm this
            # worker did not recover/complete the workflow.
            await client.get_workflow_handle(workflow_id).signal(
                VersionPinnedWorkflow.release
            )
            await asyncio.sleep(3)
            description = await client.get_workflow_handle(workflow_id).describe()
            status = description.status.name if description.status else "UNKNOWN"
            print(f"STATUS {status}", flush=True)
        else:
            assert action == "resume"
            handle = client.get_workflow_handle(workflow_id)
            print("RESULT " + await handle.result(), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
