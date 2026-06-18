"""Conformance: additional samples-python feature corpora beyond hello/ and
message_passing/, run as two processes (worker + starter) like
test_message_passing.py.

This suite deliberately mixes passing samples (a feature we support, proven
end-to-end) with xfail/skip samples (a feature that is a documented deviation),
so the conformance matrix records *why* each unsupported sample can't run — the
same "no silent gaps" philosophy as the parity ledgers.

  * context_propagation — interceptors + header propagation across
    client/workflow/activity (DEVIATIONS D24). PASSES: the runner harvests the
    client's interceptors into the Worker (the one piece temporalio does
    implicitly), then propagation is proven by the *worker* logging the
    propagated user id from inside the activity.
  * custom_converter / encryption — XFAIL: the sample's own converter/codec is
    written against the protobuf ``temporalio.api.common.v1.Payload``; our
    Payload is a lightweight dict (DEVIATIONS D1).
  * worker_specific_task_queues — XFAIL: runs two Workers in one process;
    temporal-dbos is one Worker per process (DESIGN §5).
  * sleep_for_days — SKIP: sleeps ``timedelta(days=30)`` with no auto-complete;
    needs the time-skipping WorkflowEnvironment (Phase 4).
"""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pytest

from tests.conformance.samples import ensure_samples, rewrite_package
from tests.dbconfig import system_database_url

RUNNER = Path(__file__).parent / "runner.py"


@dataclass(frozen=True)
class Sample:
    package: str  # dotted package under samples-python
    expect_output: Optional[str] = None  # substring required in starter output
    worker_expect: Optional[str] = None  # substring required in worker transcript
    ready_line: str = "orker started"  # worker readiness marker
    starter_timeout: int = 60
    ready_timeout: int = 60
    xfail: Optional[str] = None
    skip: Optional[str] = None


SAMPLES = {
    "context_propagation": Sample(
        package="context_propagation",
        # Starter logs the workflow result (via logging -> stderr).
        expect_output="Workflow result: Hello, Temporal",
        # The proof of propagation: the activity, on the worker, logs the user
        # id carried from the client header. "None" here would mean the header
        # never reached the activity.
        worker_expect="Activity called by user some-user",
    ),
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
    "sleep_for_days": Sample(
        package="sleep_for_days",
        skip="workflow sleeps timedelta(days=30) with no auto-complete; needs the "
        "time-skipping WorkflowEnvironment (Phase 4)",
    ),
}


@pytest.fixture(scope="session")
def rewritten_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    samples_root = ensure_samples()
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


@pytest.mark.timeout(240)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("sample_name", _params())
def test_feature_sample(sample_name: str, rewritten_root: Path) -> None:
    from tests.harness import PythonProcess

    sample = SAMPLES[sample_name]
    env = {
        "PYTHONPATH": f"{rewritten_root}{os.pathsep}{Path(__file__).parents[2]}",
        "TDB_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
    }

    worker = PythonProcess(RUNNER, f"{sample.package}.worker", env=env)
    worker.start()
    try:
        _wait_ready_or_die(worker, sample.ready_line, timeout=sample.ready_timeout)
        starter = subprocess.run(
            [sys.executable, str(RUNNER), f"{sample.package}.starter"],
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
