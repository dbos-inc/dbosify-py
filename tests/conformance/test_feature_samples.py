"""Conformance: additional samples-python feature corpora beyond hello/ and
message_passing/, run as two processes (worker + starter) like
test_message_passing.py — but generalized to the corpus's structural variety
(non-``worker``/``starter`` module names, workers that print no ready line,
starters that take a file argument, and starters that assert only by exiting 0).

The suite mixes passing samples (a feature we support, proven end-to-end) with
skipped samples, each carrying the documented reason it can't run here (a
fundamental deviation the sample's own code depends on, or a harness limitation),
so the conformance matrix records *why* each unsupported sample is excluded — the
same "no silent gaps" philosophy as the parity ledgers.
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
        # Proof of propagation: the activity logs the user id carried from the
        # client header, exercising interceptor header propagation end-to-end.
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
    "custom_decorator": Sample(
        package="custom_decorator",
        # An auto-heartbeat activity decorator keeps a long async activity alive
        # so a signal can cancel it cleanly rather than it dying of heartbeat timeout.
        expect_output="Result:",
        starter_timeout=30,
    ),
    "batch_sliding_window": Sample(
        package="batch_sliding_window",
        # Sliding-window batch: 90 records via ~18 continue-as-new hops + 90
        # child workflows. Runs ~68s locally, so a generous budget for slower CI.
        expect_output="Workflow completed successfully!",
        starter_timeout=180,
    ),
    "resource_pool": Sample(
        package="resource_pool",
        # A long-lived pool lends resources to user workflows via cross-workflow
        # signals. No success print — the starter exiting 0 is the proof.
        starter_timeout=60,
    ),
    # ---- skip: not runnable — a fundamental deviation the sample's own code
    #      depends on (would never pass; like hello's mTLS / multiprocess skips)
    "custom_converter": Sample(
        package="custom_converter",
        skip="sample's PayloadConverter is built on protobuf "
        "temporalio.api.common.v1.Payload; our Payload is a lightweight dict and "
        "protobuf payloads are a non-goal (ARCHITECTURE no-server)",
    ),
    "encryption": Sample(
        package="encryption",
        skip="EncryptionCodec serializes protobuf Payloads (.SerializeToString); "
        "our PayloadCodec operates on a lightweight Payload, and protobuf payloads "
        "are a non-goal (ARCHITECTURE no-server)",
    ),
    "worker_specific_task_queues": Sample(
        package="worker_specific_task_queues",
        skip="runs two Workers in one process; dbosify is one Worker per " "process",
    ),
    # ---- skip: not runnable in this harness ---------------------------------
    "sleep_for_days": Sample(
        package="sleep_for_days",
        skip="workflow sleeps timedelta(days=30) with no auto-complete; needs a "
        "time-skipping WorkflowEnvironment (out of scope)",
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
    # ---- skip: sample depends on a non-goal/unsupported Temporal feature (coverage gaps) ----
    # Nexus — non-goal (ARCHITECTURE no-server).
    "hello_nexus": Sample(
        package="hello_nexus", skip="Nexus is a non-goal (ARCHITECTURE no-server)"
    ),
    "nexus_cancel": Sample(
        package="nexus_cancel", skip="Nexus is a non-goal (ARCHITECTURE no-server)"
    ),
    "nexus_messaging": Sample(
        package="nexus_messaging", skip="Nexus is a non-goal (ARCHITECTURE no-server)"
    ),
    "nexus_multiple_args": Sample(
        package="nexus_multiple_args",
        skip="Nexus is a non-goal (ARCHITECTURE no-server)",
    ),
    # Metrics + telemetry runtime — not implemented (ARCHITECTURE no-metrics).
    "custom_metric": Sample(
        package="custom_metric",
        skip="metrics are not implemented (ARCHITECTURE no-metrics)",
    ),
    "prometheus": Sample(
        package="prometheus",
        skip="metrics / Prometheus telemetry not implemented (ARCHITECTURE no-metrics)",
    ),
    "open_telemetry": Sample(
        package="open_telemetry",
        skip="OpenTelemetry metrics/tracing runtime not implemented (ARCHITECTURE no-metrics)",
    ),
    # Client-initiated (standalone) activities — unsupported (ARCHITECTURE no-client-activities).
    "hello_standalone_activity": Sample(
        package="hello_standalone_activity",
        skip="client-initiated standalone activities are unsupported (ARCHITECTURE no-client-activities)",
    ),
    # Pydantic converter — not provided; configure a custom DataConverter.
    "pydantic_converter": Sample(
        package="pydantic_converter",
        skip="contrib.pydantic is not provided; configure a custom DataConverter",
    ),
    "pydantic_converter_v1": Sample(
        package="pydantic_converter_v1",
        skip="contrib.pydantic is not provided; configure a custom DataConverter",
    ),
    # External payload storage — not implemented.
    "external_storage": Sample(
        package="external_storage",
        skip="external payload storage is not implemented",
    ),
    "external_storage_redis": Sample(
        package="external_storage_redis",
        skip="external payload storage (redis) is not implemented",
    ),
    # Multiprocess activity worker — one Worker per process.
    "worker_multiprocessing": Sample(
        package="worker_multiprocessing",
        skip="multiprocess activity executors are unsupported",
    ),
    # Worker deployment versioning walkthrough — protobuf + 3 version-workers +
    # AUTO_UPGRADE; we enforce PINNED only (ARCHITECTURE worker-versioning).
    "worker_versioning": Sample(
        package="worker_versioning",
        skip="multi-version-worker walkthrough using protobuf + AUTO_UPGRADE; we "
        "support PINNED only (ARCHITECTURE worker-versioning)",
    ),
    # ---- skip: sample structure / non-Temporal dependency makes it un-runnable here ----
    # Patching across worker code versions (like hello_patch) — not a single run.
    "patching": Sample(
        package="patching",
        skip="multi-stage version-swap walkthrough (--start-workflow/"
        "--query-workflow across swapped worker code versions); not a single run. "
        "patched()/deprecate_patch() themselves are supported",
    ),
    # Replay — exercises our Replayer (replay_workflows is supported) but the
    # sample's replayer.py is client-only.
    "replay": Sample(
        package="replay",
        # Our Replayer re-executes checkpoints through a running Worker's DBOS runtime,
        # so it needs a Worker for the types (ARCHITECTURE replay), unlike temporalio's.
        skip="our Replayer needs a Worker for the types in the process; the "
        "sample's replayer.py is client-only (ARCHITECTURE replay)",
    ),
    # Eager workflow start — a server-side optimization (inert, no server); the
    # sample also reads the temporalio-internal __temporal_eagerly_started flag.
    "eager_wf_start": Sample(
        package="eager_wf_start",
        skip="eager start is a server-side optimization (inert here); the sample "
        "reads the temporalio-internal __temporal_eagerly_started flag",
    ),
    # Env-config TOML file/profile loading — the harness adapts envconfig away,
    # and the samples load connection config rather than run a workflow.
    "env_config": Sample(
        package="env_config",
        skip="demonstrates temporalio envconfig TOML file/profile loading (the "
        "harness stubs envconfig); runs no workflow",
    ),
    # Workflow streams — large multi-process pub/sub streaming demo.
    "workflow_streams": Sample(
        package="workflow_streams",
        skip="multi-process workflow-streams pub/sub demo (many run_* "
        "entrypoints); not a single worker+starter run",
    ),
}

# samples-python dirs covered by the other conformance suites (whole corpora).
_COVERED_ELSEWHERE = {"hello", "message_passing", "activity_worker", "schedules"}

# Dirs intentionally NOT conformance-tested: each exercises a third-party integration
# rather than a Temporal feature. Listed so the classification guard stays complete.
_NOT_CONFORMANCE_TESTABLE = {
    "bedrock",
    "openai_agents",
    "langgraph_plugin",
    "langsmith_tracing",
    "strands_plugin",
    "sentry",
    "cloud_export_to_parquet",
    "lambda_worker",
    "gevent_async",
    "trio_async",
}


@pytest.fixture(scope="session")
def samples_root() -> Path:
    return ensure_samples()


def test_every_samples_python_dir_is_classified(samples_root: Path) -> None:
    """No silent gaps in the conformance matrix: every top-level samples-python
    directory must be accounted for — run or skipped here, covered by another
    conformance suite, or explicitly not-conformance-testable. A new sample
    (or a forgotten one) fails this until it is classified."""
    all_dirs = {
        p.name
        for p in samples_root.iterdir()
        if p.is_dir() and not p.name.startswith(".") and p.name != "tests"
    }
    classified = (
        {sample.package for sample in SAMPLES.values()}
        | _COVERED_ELSEWHERE
        | _NOT_CONFORMANCE_TESTABLE
    )
    unclassified = sorted(all_dirs - classified)
    assert not unclassified, (
        f"unclassified samples-python dirs: {unclassified} — add each to "
        f"test_feature_samples.SAMPLES (run or skip), _COVERED_ELSEWHERE, or "
        f"_NOT_CONFORMANCE_TESTABLE."
    )
    stale = sorted(classified - all_dirs)
    assert not stale, (
        f"classified names not present in samples-python: {stale} — the pinned "
        f"checkout no longer has them; remove or re-point."
    )


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
        "DBOSIFY_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
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
