"""The §4.3 SIGKILL-recovery suite (DESIGN.md Phase 0 exit criteria 1, 2, 4,
and 5), driven through real worker subprocesses against real Postgres. The
non-kill criteria (3 and 6) live in test_interpreter_basic.py.
"""

import json
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional

import pytest
from dbos import DBOSClient

from dbosify._internal import conversion, inbox
from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "phase0_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}

PERF_ITERATIONS = 1000


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


class Driver:
    """Test-side client for a phase0_worker subprocess: signals/updates via
    DBOSClient envelopes, exactly like a cross-process Temporal client.
    """

    def __init__(self) -> None:
        self._client: Optional[DBOSClient] = None

    @property
    def client(self) -> DBOSClient:
        if self._client is None:
            self._client = DBOSClient(system_database_url=system_database_url())
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.destroy()
            self._client = None

    def signal(self, workflow_id: str, name: str, args: List[Any] = []) -> None:
        # Encode args like the Client facade (this Driver is a cross-process
        # client; the worker's interpreter decodes against the handler sig).
        self.client.send(
            workflow_id,
            inbox.signal_envelope(name, conversion.encode_values_sync(args)),
            inbox.INBOX_TOPIC,
        )

    def update(
        self, workflow_id: str, name: str, args: List[Any] = [], *, timeout: float = 30
    ) -> Any:
        update_id = str(uuid.uuid4())
        self.client.send(
            workflow_id,
            inbox.update_envelope(name, conversion.encode_values_sync(args), update_id),
            inbox.INBOX_TOPIC,
        )
        reply = self.client.get_event(
            workflow_id, inbox.update_result_key(update_id), timeout
        )
        assert reply is not None, f"update {name} timed out"
        if isinstance(reply, dict) and "result" in reply:
            # A completed update's result is encoded (the Client decodes it
            # against the handler signature; here we have no hint).
            reply["result"] = conversion.decode_value_sync(reply["result"])
        return reply


@pytest.fixture()
def driver() -> Any:
    d = Driver()
    yield d
    d.close()


def _spawn(mode: str, workflow_id: str, *extra: str) -> PythonProcess:
    proc = PythonProcess(WORKER, mode, workflow_id, *extra, env=ENV)
    proc.start()
    return proc


@pytest.mark.usefixtures("cleanup_test_databases")
def test_signal_recovery_not_applied_twice(driver: Driver) -> None:
    """§4.3 test 1: kill after the signal is recorded but before completion;
    after recovery the result is the same and the handler was not re-applied
    twice to state.
    """
    first = _spawn("approval-start", "approval-wf")
    try:
        first.wait_for_line("STARTED")
        driver.signal("approval-wf", "approve")
        # APPROVED prints only after the signal's recv checkpoint committed
        # and the handler ran; the workflow is still RUNNING.
        first.wait_for_line("APPROVED")
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = _spawn("approval-resume", "approval-wf")
    try:
        # Recovery replays the same signal delivery (and the handler exactly
        # once): the workflow parks again awaiting the go signal.
        second.wait_for_line("APPROVED", timeout=60)
        driver.signal("approval-wf", "go")
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == 1  # exactly one approval applied, not two


@pytest.mark.usefixtures("cleanup_test_databases")
def test_race_order_identical_across_recovery(driver: Driver) -> None:
    """§4.3 test 2: two concurrent activities + a timer racing; the
    completion order observed by user code is identical across a forced
    recovery replay.
    """
    first = _spawn("race-start", "race-wf")
    try:
        first.wait_for_line("RACED", timeout=60)
        before = driver.update("race-wf", "get_order")
        assert before["status"] == "completed"
        order_before = before["result"]
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = _spawn("race-resume", "race-wf")
    try:
        second.wait_for_line("RACED", timeout=60)
        after = driver.update("race-wf", "get_order")
        assert after["status"] == "completed"
        order_after = after["result"]
        driver.signal("race-wf", "go")
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert sorted(order_before) == ["fast", "slow", "timer"]
    # The determinism claim: replayed state matches pre-kill state exactly,
    # and the final result matches both.
    assert order_after == order_before
    assert result == order_before


@pytest.mark.usefixtures("cleanup_test_databases")
def test_rejected_update_leaves_no_trace_across_recovery(driver: Driver) -> None:
    """§4.3 test 4: a rejected update leaves no trace in workflow state
    across recovery; an accepted update returns its value.
    """
    first = _spawn("counter-start", "counter-wf")
    try:
        first.wait_for_line("STARTED")
        accepted = driver.update("counter-wf", "add", [5])
        assert accepted == {"status": "completed", "result": 5}
        rejected = driver.update("counter-wf", "add", [-1])
        assert rejected["status"] == "rejected"
        assert rejected["failure"]["type"] == "BadAmount"
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = _spawn("counter-resume", "counter-wf")
    try:
        second.wait_for_line("STARTED")
        # Replayed state must be 5 (the rejection applied nothing), so a new
        # accepted update lands on 7.
        after = driver.update("counter-wf", "add", [2], timeout=60)
        assert after == {"status": "completed", "result": 7}
        driver.signal("counter-wf", "finish")
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == 7


@pytest.mark.timeout(600)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_perf_baseline_1000_iterations(driver: Driver) -> None:
    """§4.3 test 5: 1,000 iterations of (sleep(0) + tiny activity); measure
    first-execution and recovery-replay times. Numbers go in docs/perf.md;
    the only hard assertion is correctness.
    """
    first = _spawn("perf-start", "perf-wf", str(PERF_ITERATIONS))
    try:
        first.wait_for_line("STARTED", timeout=60)
        t0 = time.monotonic()
        first.wait_for_line("LOOP_DONE", timeout=300)
        first_execution_seconds = time.monotonic() - t0
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = _spawn("perf-resume", "perf-wf")
    try:
        second.wait_for_line("LAUNCHED", timeout=60)
        t0 = time.monotonic()
        second.wait_for_line("LOOP_DONE", timeout=300)
        replay_seconds = time.monotonic() - t0
        driver.signal("perf-wf", "go")
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == sum(range(PERF_ITERATIONS))
    print(
        f"\nPERF_BASELINE iterations={PERF_ITERATIONS} "
        f"first={first_execution_seconds:.2f}s "
        f"replay={replay_seconds:.2f}s "
        f"per_iter_first={first_execution_seconds / PERF_ITERATIONS * 1000:.2f}ms "
        f"per_iter_replay={replay_seconds / PERF_ITERATIONS * 1000:.2f}ms",
        flush=True,
    )
