"""Conformance: additional samples-python feature corpora beyond hello/ and
message_passing/, run as two processes (worker + starter) like
test_message_passing.py — but generalized to the corpus's structural variety
(non-``worker``/``starter`` module names, workers that print no ready line,
starters that take a file argument, and starters that assert only by exiting 0).

The suite mixes passing samples (a feature we support, proven end-to-end) with
xfail/skip samples (a documented deviation), so the conformance matrix records
*why* each unsupported sample can't run — the same "no silent gaps" philosophy
as the parity ledgers.
"""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

import pytest

from tests.conformance.samples import ensure_samples, rewrite_package
from tests.dbconfig import system_database_url

RUNNER = Path(__file__).parent / "runner.py"


@dataclass(frozen=True)
class Sample:
    package: str  # dotted package tree to rewrite under samples-python
    worker_module: str = ""  # dotted worker module (default: f"{package}.worker")
    starter_module: str = ""  # dotted starter module (default: f"{package}.starter")
    expect_output: Optional[str] = None  # substring required in starter output
    worker_expect: Optional[str] = None  # substring required in worker transcript
    # Every worker logs "DBOS launched!" on launch, so it's a universal ready
    # marker even for samples that print nothing themselves.
    ready_line: str = "DBOS launched!"
    # Files (relative to the samples-python root) passed as extra starter argv,
    # resolved to absolute paths (e.g. dsl's YAML).
    starter_arg_files: Tuple[str, ...] = ()
    starter_timeout: int = 90
    ready_timeout: int = 60
    xfail: Optional[str] = None
    skip: Optional[str] = None

    def worker(self) -> str:
        return self.worker_module or f"{self.package}.worker"

    def starter(self) -> str:
        return self.starter_module or f"{self.package}.starter"


SAMPLES = {
    # ---- pass: a supported feature, proven end-to-end -----------------------
    "context_propagation": Sample(
        package="context_propagation",
        # Starter logs the workflow result (via logging -> stderr).
        expect_output="Workflow result: Hello, Temporal",
        # Proof of propagation: the activity, on the worker, logs the user id
        # carried from the client header ("None" here would mean the header
        # never reached the activity). Exercises D24 end-to-end.
        worker_expect="Activity called by user some-user",
    ),
    "polling_frequent": Sample(
        package="polling",
        worker_module="polling.frequent.run_worker",
        starter_module="polling.frequent.run_frequent",
        # A heartbeating activity polls a flaky service until it succeeds.
        expect_output="Result:",
    ),
    "updatable_timer": Sample(
        package="updatable_timer",
        # Starts a workflow parked on an updatable timer; the starter proves the
        # start path (the timer-update demo is a separate manual process).
        expect_output="Workflow started: run_id=",
    ),
    # ---- xfail: a documented deviation --------------------------------------
    "custom_converter": Sample(
        package="custom_converter",
        xfail="sample's PayloadConverter is built on protobuf "
        "temporalio.api.common.v1.Payload; our Payload is a lightweight dict "
        "(DEVIATIONS D1)",
        ready_timeout=25,
        starter_timeout=25,
    ),
    "encryption": Sample(
        package="encryption",
        xfail="EncryptionCodec serializes protobuf Payloads (.SerializeToString); "
        "our PayloadCodec operates on a lightweight Payload (DEVIATIONS D1)",
        ready_timeout=25,
        starter_timeout=25,
    ),
    "worker_specific_task_queues": Sample(
        package="worker_specific_task_queues",
        xfail="runs two Workers in one process; temporal-dbos is one Worker per "
        "process (DESIGN §5)",
        ready_timeout=25,
        starter_timeout=25,
    ),
    # ---- xfail: a supported feature that does NOT pass yet — surfaced by this
    #      sample as a suspected bug/limitation, worth investigating separately
    #      (NOT an intentional deviation) -------------------------------------
    "custom_decorator": Sample(
        package="custom_decorator",
        # The auto-heartbeater heartbeats every 1s under a 2s heartbeat timeout,
        # yet the activity still dies of HEARTBEAT timeout — the watchdog isn't
        # reset by heartbeats issued from the decorator's background task.
        xfail="activity hits HEARTBEAT timeout despite the auto-heartbeater's 1s "
        "heartbeats (2s timeout) — suspected heartbeat-watchdog bug",
        starter_timeout=30,
    ),
    "batch_sliding_window": Sample(
        package="batch_sliding_window",
        xfail="continue-as-new + child sliding-window batch does not complete "
        "within the test budget (CAN-chain perf or hang — needs investigation)",
        ready_timeout=25,
        starter_timeout=30,
    ),
    "resource_pool": Sample(
        package="resource_pool",
        xfail="resource acquisition via cross-workflow signals does not complete "
        "within the test budget (deadlock/perf — needs investigation)",
        ready_timeout=25,
        starter_timeout=30,
    ),
    # ---- skip: not runnable in this harness ---------------------------------
    "sleep_for_days": Sample(
        package="sleep_for_days",
        skip="workflow sleeps timedelta(days=30) with no auto-complete; needs the "
        "time-skipping WorkflowEnvironment (Phase 4)",
    ),
    "polling_infrequent": Sample(
        package="polling",
        skip="infrequent polling retries the activity every 60s for 5 attempts "
        "(~4min); too slow for CI",
    ),
    "polling_periodic_sequence": Sample(
        package="polling",
        skip="periodic polling continues-as-new forever; execute_workflow never "
        "returns (a runs-forever sample, like hello_cron)",
    ),
    "dsl": Sample(
        package="dsl",
        skip="requires the sample's own third-party dependency (dacite); the "
        "harness rewrites imports but does not install per-sample deps",
    ),
}


@pytest.fixture(scope="session")
def samples_root() -> Path:
    return ensure_samples()


@pytest.fixture(scope="session")
def rewritten_root(
    tmp_path_factory: pytest.TempPathFactory, samples_root: Path
) -> Path:
    dest = tmp_path_factory.mktemp("rewritten_feature_samples")
    for sample in SAMPLES.values():
        if sample.skip:
            continue
        rewrite_package(samples_root, sample.package, dest)
    return dest


def _params() -> "list[Any]":
    params = []
    for name, sample in sorted(SAMPLES.items()):
        marks = []
        if sample.skip:
            marks.append(pytest.mark.skip(reason=sample.skip))
        elif sample.xfail:
            marks.append(pytest.mark.xfail(reason=sample.xfail, strict=False))
        params.append(pytest.param(name, id=name, marks=marks))
    return params


def _wait_ready_or_die(worker: Any, ready_line: str, timeout: float) -> None:
    """Wait for the worker's ready line, but fail fast if the worker process
    exits first (xfail samples often die at startup — don't burn the whole
    timeout waiting for a line that will never come)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        proc = worker._proc
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(
                f"worker exited early (code {proc.returncode}) before readiness:\n"
                f"{''.join(worker.transcript[-40:])}"
            )
        try:
            worker.wait_for_line(ready_line, timeout=1.0)
            return
        except TimeoutError:
            continue
    raise TimeoutError(
        f"worker not ready within {timeout}s:\n{''.join(worker.transcript[-40:])}"
    )


@pytest.mark.timeout(600)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("sample_name", _params())
def test_feature_sample(
    sample_name: str, rewritten_root: Path, samples_root: Path
) -> None:
    from tests.harness import PythonProcess

    sample = SAMPLES[sample_name]
    env = {
        "PYTHONPATH": f"{rewritten_root}{os.pathsep}{Path(__file__).parents[2]}",
        "TDB_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
    }
    starter_args = [str(samples_root / f) for f in sample.starter_arg_files]

    worker = PythonProcess(RUNNER, sample.worker(), env=env)
    worker.start()
    try:
        _wait_ready_or_die(worker, sample.ready_line, timeout=sample.ready_timeout)
        starter = subprocess.run(
            [sys.executable, str(RUNNER), sample.starter(), *starter_args],
            capture_output=True,
            text=True,
            timeout=sample.starter_timeout,
            env={**os.environ, **env},
        )
        combined = starter.stdout + starter.stderr
        assert starter.returncode == 0, (
            f"starter exited {starter.returncode}\n"
            f"--- stdout ---\n{starter.stdout}\n"
            f"--- stderr ---\n{starter.stderr[-4000:]}\n"
            f"--- worker tail ---\n{''.join(worker.transcript[-30:])}"
        )
        if sample.expect_output:
            assert sample.expect_output in combined, (
                f"expected {sample.expect_output!r} in starter output\n"
                f"--- combined ---\n{combined[-2000:]}"
            )
        if sample.worker_expect:
            transcript = "".join(worker.transcript)
            assert sample.worker_expect in transcript, (
                f"expected {sample.worker_expect!r} in worker transcript "
                f"(propagation proof)\n--- worker tail ---\n"
                f"{''.join(worker.transcript[-40:])}"
            )
    finally:
        worker.terminate_and_wait()
