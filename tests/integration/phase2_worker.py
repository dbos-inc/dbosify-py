"""Subprocess worker for Phase 2 recovery tests.

Run as: python phase2_worker.py <scenario>-<start|resume> <workflow_id> <effects_path>

Scenarios:
  cancel  cancellation unwind: parks forever; on cancel the unwind runs a
          cleanup activity then parks awaiting `go` — the kill window is
          mid-unwind, after the cleanup checkpoint.
  child   child re-attach: parent starts a slow recording child and awaits
          it — the kill window is after the child started, before it
          completed; recovery must re-attach, not spawn a twin.
  replay  is_replaying probe: samples workflow.unsafe.is_replaying() before
          an activity, after it, and after a post-recovery signal — the kill
          window is while parked, so recovery replays the prefix (True)
          and the signal-driven tail is live (False).
  updates message-handler chaos: two slow updates accepted (durable
          acceptance events) and parked, a signal interleaved between them —
          the kill window is with handlers mid-flight; recovery must replay
          delivery order, re-park the handlers, run each effect exactly
          once, and still answer both update results. Driven by the test
          process via the public client; this worker only hosts.
"""

import asyncio
import json
import sys
from datetime import timedelta

from dbos import DBOSClient

from temporal_dbos import activity, workflow
from temporal_dbos.client import Client, WorkflowFailureError
from temporal_dbos.common import RetryPolicy
from temporal_dbos.exceptions import ApplicationError, ChildWorkflowError
from temporal_dbos.worker import Worker
from tests.dbconfig import default_config, system_database_url

TASK_QUEUE = "phase2-recovery-tq"


@activity.defn
async def record_cleanup(path: str) -> None:
    print("CLEANUP_ACTIVITY_EXECUTED", flush=True)
    with open(path, "a") as f:
        f.write("cleanup\n")


@activity.defn
async def record_child_work(path: str) -> str:
    print("CHILD_ACTIVITY_EXECUTED", flush=True)
    with open(path, "a") as f:
        f.write("child-work\n")
    return "done"


@workflow.defn
class SlowChild:
    @workflow.run
    async def run(self, path: str) -> str:
        print("CHILD_STARTED", flush=True)
        await workflow.sleep(3.0)
        result: str = await workflow.execute_activity(
            record_child_work, path, start_to_close_timeout=timedelta(seconds=10)
        )
        return result


@workflow.defn
class ChildParent:
    @workflow.run
    async def run(self, path: str) -> str:
        result: str = await workflow.execute_child_workflow(
            SlowChild.run, path, id="reattach-child"
        )
        return f"parent saw: {result}"


@workflow.defn
class RetryRecoveryChild:
    """Fails on attempt 1, succeeds (and records, exactly once) on attempt 2.
    The 3s sleep gives a deterministic SIGKILL window during attempt 1."""

    @workflow.run
    async def run(self, path: str) -> str:
        attempt = workflow.info().attempt
        print(f"CHILD_ATTEMPT {attempt}", flush=True)
        await workflow.sleep(3.0)
        if attempt < 2:
            raise ApplicationError(f"child fail attempt {attempt}")
        result: str = await workflow.execute_activity(
            record_child_work, path, start_to_close_timeout=timedelta(seconds=10)
        )
        return result


@workflow.defn
class RetryChildParent:
    @workflow.run
    async def run(self, path: str) -> str:
        result: str = await workflow.execute_child_workflow(
            RetryRecoveryChild.run,
            path,
            id="reattach-retry-child",
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50), maximum_attempts=5
            ),
        )
        return f"parent saw: {result}"


@workflow.defn
class TimeoutRecoveryChild:
    """Sleeps far longer than its run_timeout. The deadline is durable, so a
    SIGKILL + recovery must still terminate it at the original deadline rather
    than letting the recovered run sleep out the full 120s."""

    @workflow.run
    async def run(self) -> str:
        print("TIMEOUT_CHILD_STARTED", flush=True)
        await workflow.sleep(120)
        return "should-not-finish"


@workflow.defn
class TimeoutChildParent:
    @workflow.run
    async def run(self, path: str) -> str:
        try:
            await workflow.execute_child_workflow(
                TimeoutRecoveryChild.run,
                id="reattach-timeout-child",
                run_timeout=timedelta(seconds=8),
            )
            return "no-timeout"
        except ChildWorkflowError as err:
            return f"child-terminated:{type(err.cause).__name__}"


@activity.defn
async def record_update_effect(path: str, n: int) -> int:
    print(f"UPDATE_EFFECT {n}", flush=True)
    with open(path, "a") as f:
        f.write(f"u{n}\n")
    return n


@workflow.defn
class ReplayProbeWorkflow:
    def __init__(self) -> None:
        self.proceed = False

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.run
    async def run(self, path: str) -> "list[bool]":
        early = workflow.unsafe.is_replaying()
        # Suppressed during replay (workflow.logger's default), so the
        # resume process must not re-emit it.
        workflow.logger.warning("probe-log-early")
        await workflow.execute_activity(
            record_cleanup, path, start_to_close_timeout=timedelta(seconds=10)
        )
        # Past the last checkpointed event: the live frontier.
        mid = workflow.unsafe.is_replaying()
        print("PROBE_PARKED", flush=True)
        await workflow.wait_condition(lambda: self.proceed)
        late = workflow.unsafe.is_replaying()
        workflow.logger.warning("probe-log-late")
        return [early, mid, late]


@workflow.defn
class UpdateChaosWorkflow:
    def __init__(self) -> None:
        self.history: "list[str]" = []
        self.release = False
        self.done = False

    @workflow.signal
    def mark(self, label: str) -> None:
        self.history.append(f"sig:{label}")

    @workflow.signal
    def release_updates(self) -> None:
        self.release = True

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.update
    async def slow_update(self, path: str, n: int) -> int:
        self.history.append(f"upd-start:{n}")
        await workflow.wait_condition(lambda: self.release)
        await workflow.execute_activity(
            record_update_effect,
            args=[path, n],
            start_to_close_timeout=timedelta(seconds=10),
        )
        self.history.append(f"upd-end:{n}")
        return n

    @slow_update.validator
    def _validate_slow_update(self, path: str, n: int) -> None:
        # Side effects prove run-once: the verdict is checkpointed, so
        # replay must not re-execute the validator (Temporal semantics).
        print(f"VALIDATOR_RAN {n}", flush=True)
        with open(path, "a") as f:
            f.write(f"v{n}\n")
        if n < 0:
            raise ValueError("no negatives")

    @workflow.run
    async def run(self, path: str) -> "list[str]":
        await workflow.wait_condition(
            lambda: self.done and workflow.all_handlers_finished()
        )
        return self.history


@workflow.defn
class CleanupHoldWorkflow:
    def __init__(self) -> None:
        self.proceed = False

    @workflow.signal
    def go(self) -> None:
        self.proceed = True

    @workflow.run
    async def run(self, path: str) -> None:
        try:
            await workflow.wait_condition(lambda: False)
        finally:
            await workflow.execute_activity(
                record_cleanup, path, start_to_close_timeout=timedelta(seconds=10)
            )
            print("CLEANUP_DONE", flush=True)
            # Hold the unwind open so the test can SIGKILL mid-cancellation.
            await workflow.wait_condition(lambda: self.proceed)


async def main() -> None:
    mode, workflow_id, effects_path = sys.argv[1], sys.argv[2], sys.argv[3]
    scenario, _, action = mode.partition("-")
    run_refs = {
        "cancel": CleanupHoldWorkflow.run,
        "child": ChildParent.run,
        "childretry": RetryChildParent.run,
        "childtimeout": TimeoutChildParent.run,
        "replay": ReplayProbeWorkflow.run,
        "updates": UpdateChaosWorkflow.run,
    }
    run_ref = run_refs[scenario]
    async with Worker(
        default_config(),
        task_queue=TASK_QUEUE,
        workflows=[
            CleanupHoldWorkflow,
            ChildParent,
            SlowChild,
            RetryChildParent,
            RetryRecoveryChild,
            TimeoutChildParent,
            TimeoutRecoveryChild,
            ReplayProbeWorkflow,
            UpdateChaosWorkflow,
        ],
        activities=[record_cleanup, record_child_work, record_update_effect],
    ):
        dbos_client = DBOSClient(system_database_url=system_database_url())
        try:
            client = await Client.connect(dbos_client)
            if action == "start":
                handle = await client.start_workflow(
                    run_ref,
                    effects_path,
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                )
            else:
                assert action == "resume"
                handle = client.get_workflow_handle(workflow_id)
            print("STARTED", flush=True)
            try:
                result = await handle.result()
                outcome = {"result": result}
            except WorkflowFailureError as err:
                outcome = {"cause": type(err.cause).__name__}
            status = (await handle.describe()).status
            assert status is not None
            print(
                "RESULT " + json.dumps({**outcome, "status": status.name}), flush=True
            )
        finally:
            dbos_client.destroy()


if __name__ == "__main__":
    asyncio.run(main())
