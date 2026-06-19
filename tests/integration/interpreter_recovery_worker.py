"""Subprocess worker hosting the §4.3 exit-criteria workflows.

Run as: python interpreter_recovery_worker.py <mode> <workflow_id> [extra]

Modes (X-start launches and starts the workflow; X-resume launches and lets
DBOS recovery re-execute it):
  approval   §4.3 test 1 — wait_condition + signal, kill between signal and
             completion
  race       §4.3 test 2 — two activities + a timer racing; completion order
             must replay identically
  counter    §4.3 test 4 — update validator; rejected updates leave no trace
             across recovery
  perf       §4.3 test 5 — N iterations of (sleep(0) + tiny activity);
             recovery replay time is the perf baseline
"""

import asyncio
import json
import sys
from datetime import timedelta
from typing import Any, List, Optional

from dbos import DBOS

from dbosify import activity, workflow
from dbosify._internal import dispatcher
from dbosify.exceptions import ApplicationError
from tests.dbconfig import default_config

ACTIVITY_TIMEOUT = timedelta(seconds=10)


@activity.defn
async def timed_activity(name: str, duration: float) -> str:
    await asyncio.sleep(duration)
    return name


@activity.defn
async def tiny_activity(i: int) -> int:
    return i


@workflow.defn
class ApprovalWorkflow:
    def __init__(self) -> None:
        self.approvals = 0
        self.proceed = False

    @workflow.signal
    def approve(self) -> None:
        self.approvals += 1

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self.approvals > 0)
        # By this print, the signal's recv checkpoint is durable but the
        # workflow is not complete — the kill window for test 1.
        print("APPROVED", flush=True)
        await workflow.wait_condition(lambda: self.proceed)
        return self.approvals


@workflow.defn
class RaceWorkflow:
    def __init__(self) -> None:
        self.order: List[str] = []
        self.proceed = False

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.update
    def get_order(self) -> List[str]:
        return list(self.order)

    @workflow.run
    async def run(self) -> List[str]:
        async def run_activity(name: str, duration: float) -> None:
            await workflow.execute_activity(
                timed_activity,
                args=[name, duration],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
            self.order.append(name)

        async def run_timer(name: str, duration: float) -> None:
            await workflow.sleep(duration)
            self.order.append(name)

        await asyncio.gather(
            run_activity("slow", 0.5),
            run_activity("fast", 0.1),
            run_timer("timer", 0.3),
        )
        print("RACED", flush=True)
        await workflow.wait_condition(lambda: self.proceed)
        return list(self.order)


@workflow.defn
class CounterWorkflow:
    def __init__(self) -> None:
        self.total = 0
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.update
    def add(self, n: int) -> int:
        self.total += n
        return self.total

    @add.validator
    def _validate_add(self, n: int) -> None:
        if n < 0:
            raise ApplicationError("negative amounts not allowed", type="BadAmount")

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self.done)
        return self.total


@workflow.defn
class PerfWorkflow:
    def __init__(self) -> None:
        self.proceed = False

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.run
    async def run(self, iterations: int) -> int:
        total = 0
        for i in range(iterations):
            await workflow.sleep(0)
            total += await workflow.execute_activity(
                tiny_activity, i, start_to_close_timeout=ACTIVITY_TIMEOUT
            )
        print("LOOP_DONE", flush=True)
        await workflow.wait_condition(lambda: self.proceed)
        return total


WORKFLOWS = {
    "approval": ApprovalWorkflow,
    "race": RaceWorkflow,
    "counter": CounterWorkflow,
    "perf": PerfWorkflow,
}


def main() -> None:
    mode, workflow_id = sys.argv[1], sys.argv[2]
    extra = sys.argv[3] if len(sys.argv) > 3 else None
    kind, _, action = mode.partition("-")

    DBOS(config=default_config())
    dispatcher.register_worker(
        workflows=list(WORKFLOWS.values()), activities=[timed_activity, tiny_activity]
    )
    DBOS.launch()  # on resume, recovery re-executes the pending workflow
    print("LAUNCHED", flush=True)

    handle: Any
    if action == "start":
        args: List[Any] = [int(extra)] if kind == "perf" and extra else []
        handle = dispatcher.start_workflow(
            WORKFLOWS[kind], args, workflow_id=workflow_id
        )
    else:
        assert action == "resume", f"unknown action {action}"
        handle = DBOS.retrieve_workflow(workflow_id)
    print("STARTED", flush=True)

    # get_result() returns the encoded result envelope; decode it (the
    # converter boundary, same as the Client facade does).
    result = dispatcher.workflow_result(handle)
    print("RESULT " + json.dumps(result), flush=True)
    DBOS.destroy()


if __name__ == "__main__":
    main()
