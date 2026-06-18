"""Subprocess worker: a dynamic-handler workflow that continues-as-new, then
gets SIGKILLed while the post-CAN run is parked.

Run as: python dynamic_can_recovery_worker.py <start|resume> <workflow_id>

``start`` records one signal via the catch-all handler (``kept``), hops (CAN)
carrying that state into run 1, which announces "HOPPED" and parks. The test
SIGKILLs there. On resume, DBOS recovery resumes run 1 from its checkpoints
(its carried state intact), the worker sends one more unknown-named signal
(``added``) to the catch-all handler and finishes, and reports the accumulated
state — proving CAN + dynamic-handler delivery survive the crash exactly once.
"""

import asyncio
import json
import sys
from typing import List, Optional, Sequence

from dbos import DBOSClient

from temporal_dbos import workflow
from temporal_dbos.client import Client
from temporal_dbos.common import RawValue
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "dynamic-can-recovery-tq"


@workflow.defn
class DynamicCANRecoveryWorkflow:
    def __init__(self) -> None:
        self.seen: List[str] = []
        self.hop = False
        self.done = False

    @workflow.signal(dynamic=True)
    def any_signal(self, name: str, args: Sequence[RawValue]) -> None:
        if name == "hop":
            self.hop = True
        elif name == "finish":
            self.done = True
        else:
            pc = workflow.payload_converter()
            val = pc.from_payload(args[0].payload) if args else None
            self.seen.append(f"{name}:{val}")

    @workflow.run
    async def run(self, carried: Optional[List[str]] = None) -> List[str]:
        self.seen = (carried or []) + self.seen
        if workflow.info().continued_run_id is not None:
            # The post-CAN run: let the chain hop checkpoint durably, announce,
            # and park for the SIGKILL.
            await workflow.sleep(0.3)
            print("HOPPED", flush=True)
        await workflow.wait_condition(lambda: self.hop or self.done)
        if self.hop:
            workflow.continue_as_new(self.seen)
        return self.seen


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[DynamicCANRecoveryWorkflow],
        activities=[],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = Client(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    DynamicCANRecoveryWorkflow.run,
                    [],
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                await handle.signal("kept", "K")  # run 0, catch-all handler
                await handle.signal("hop")  # run 0 hops (CAN) carrying ["kept:K"]
                print("STARTED", flush=True)
                # Follow the chain and park; the test SIGKILLs once it sees HOPPED.
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal("added", "A")  # run 1, post-recovery
                await handle.signal("finish")
                print("RESULT " + json.dumps(await handle.result()), flush=True)
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
