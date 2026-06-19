"""Subprocess worker for dynamic-signal-handler recovery.

Run as: python dynamic_handlers_recovery_worker.py <start|resume> <workflow_id>

The workflow has only a *dynamic* signal handler. ``start`` launches it and
sends one unknown-named signal (``ping``), which the catch-all handler records
into workflow state; the run waits for that delivery, lets the checkpointed
inbox recv settle, announces "DELIVERED", and parks. The test SIGKILLs there.

On resume, DBOS recovery replays the checkpointed ``ping`` delivery through the
dynamic handler (rebuilding state), the worker sends a second unknown signal
(``finish``) to release the run, and reports everything the handler recorded —
proving the dynamic signal was delivered exactly once across the crash.
"""

import asyncio
import json
import sys
from typing import Any, List, Sequence

from dbos import DBOSClient

from dbosify import workflow
from dbosify.client import Client
from dbosify.common import RawValue
from dbosify.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "dynamic-recovery-tq"


@workflow.defn
class DynamicRecoveryWorkflow:
    def __init__(self) -> None:
        self.received: List[List[Any]] = []
        self.done = False

    @workflow.signal(dynamic=True)
    def any_signal(self, name: str, args: Sequence[RawValue]) -> None:
        pc = workflow.payload_converter()
        self.received.append([name, [pc.from_payload(a.payload) for a in args]])
        if name == "finish":
            self.done = True

    @workflow.run
    async def run(self) -> List[List[Any]]:
        await workflow.wait_condition(lambda: len(self.received) >= 1)
        # Let the inbox recv checkpoint durably before we announce + park.
        await workflow.sleep(0.3)
        print("DELIVERED", flush=True)
        await workflow.wait_condition(lambda: self.done)
        return self.received


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[DynamicRecoveryWorkflow],
        activities=[],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    DynamicRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                # Send an unknown-named signal → the catch-all handler.
                await handle.signal("ping", 5)
                print("STARTED", flush=True)
                # Park until the run closes (it won't until resume sends finish);
                # the test SIGKILLs this process once it sees DELIVERED.
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal("finish")
                received = await handle.result()
                print("RESULT " + json.dumps(received), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
