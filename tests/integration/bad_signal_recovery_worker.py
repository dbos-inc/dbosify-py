"""Subprocess worker proving an un-deserializable signal stays dropped across a
crash (and well-typed signals survive).

Run as: python bad_signal_recovery_worker.py <start|resume> <workflow_id>

A signal whose payload can't be deserialized to the handler's parameter type is
logged and dropped; the workflow keeps running. The drop is deterministic
(payload + handler signature), so it must replay identically. The test sends one
bad signal and one good signal, waits until only the good one is recorded, then
SIGKILLs. On resume, recovery replays run() — re-delivering and re-dropping the
bad signal and re-applying the good one — and a final ``finish`` signal releases
the run, which must return exactly the well-typed signals.
"""

import asyncio
import sys
from dataclasses import dataclass

from dbosify import activity, workflow  # noqa: F401  (workflow used below)
from dbosify.worker import Worker
from tests.dbconfig import connect_client, default_config

TASK_QUEUE = "bad-signal-recovery-tq"


@dataclass
class SigParam:
    some_str: str


@workflow.defn(name="BadSignalRecoveryWorkflow")
class BadSignalRecoveryWorkflow:
    def __init__(self) -> None:
        self._signals: list[SigParam] = []

    @workflow.run
    async def run(self) -> list[str]:
        await workflow.wait_condition(
            lambda: bool(self._signals) and self._signals[-1].some_str == "finish"
        )
        return [s.some_str for s in self._signals]

    @workflow.signal
    async def some_signal(self, param: SigParam) -> None:
        self._signals.append(param)

    @workflow.query
    def count(self) -> int:
        return len(self._signals)


async def main() -> None:
    action, workflow_id = sys.argv[1], sys.argv[2]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[BadSignalRecoveryWorkflow],
    ):
        client = await connect_client()
        try:
            if action == "start":
                handle = await client.start_workflow(
                    BadSignalRecoveryWorkflow.run,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
                # Wrong type (dropped) then well-typed (recorded).
                await handle.signal("some_signal", "bad")
                await handle.signal(
                    BadSignalRecoveryWorkflow.some_signal, SigParam("good")
                )
                while await handle.query(BadSignalRecoveryWorkflow.count) < 1:
                    await asyncio.sleep(0.1)
                print("PARKED", flush=True)
                await handle.result()
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
                await handle.signal(
                    BadSignalRecoveryWorkflow.some_signal, SigParam("finish")
                )
                print("RESULT " + ",".join(await handle.result()), flush=True)
        finally:
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
