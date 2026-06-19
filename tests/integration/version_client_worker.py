"""Subprocess worker for the separate-process-client versioning test
(DEVIATIONS worker-versioning). Runs a Worker on ``$DBOSIFY_BUILD_ID`` hosting a trivial echo
workflow, prints ``READY``, and runs until killed. The *test* process acts as a
bare client (it never sets this build id) and starts the workflow.
"""

import asyncio
import os

from dbos import DBOS

from dbosify import workflow
from dbosify.worker import Worker
from tests.dbconfig import default_config

TASK_QUEUE = "version-client-tq"


@workflow.defn(name="VersionEcho")
class VersionEcho:
    @workflow.run
    async def run(self, msg: str) -> str:
        return msg


async def main() -> None:
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[VersionEcho],
        build_id=os.environ["DBOSIFY_BUILD_ID"],
    ):
        for _ in range(500):
            if await DBOS.retrieve_queue_async(TASK_QUEUE) is not None:
                break
            await asyncio.sleep(0.02)
        print("READY", flush=True)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
