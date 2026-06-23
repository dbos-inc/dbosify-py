"""Long-lived worker for the cross-process query-on-closed test.

It registers ``GreetingWf`` and then stays up, running no workflow itself, so a
*pure client* in another process — one with no co-located worker — can start a
workflow here, let it close, and then query it. The query rehydrates the closed
run by forking it; that fork is dequeued and served by THIS worker. That proves
query-on-closed needs only *some* worker for the type to be running (as in
Temporal), not one in the querying process.

Run as: python rehydrate_xproc_worker.py
"""

import asyncio

from dbosify import workflow
from dbosify.worker import Worker
from tests.dbconfig import default_config

TASK_QUEUE = "rehydrate-xproc-tq"


@workflow.defn
class GreetingWf:
    def __init__(self) -> None:
        self._greeting = "<none>"

    @workflow.query
    def greeting(self) -> str:
        return self._greeting

    @workflow.run
    async def run(self, name: str) -> str:
        self._greeting = f"Hello, {name}!"
        await workflow.sleep(0.05)
        self._greeting = f"Goodbye, {name}!"
        return self._greeting


async def main() -> None:
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[GreetingWf],
        activities=[],
    ):
        print("READY", flush=True)
        # Stay alive until the test tears us down; the worker keeps polling the
        # task queue, so it dequeues and serves the client's rehydrate fork.
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
