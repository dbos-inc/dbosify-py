"""Conformance: run temporalio/samples-python ``hello/`` samples against
temporal-dbos with the mechanical import rewrite plus the connection-setup
adapter (see runner.py). The pass/xfail expectations below are the
conformance table published in the README — xfail reasons name the roadmap
phase that unblocks each sample.
"""

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pytest

from tests.conformance.samples import ensure_samples, rewrite_sample
from tests.dbconfig import system_database_url

RUNNER = Path(__file__).parent / "runner.py"
SAMPLE_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class Expectation:
    expect_output: Optional[str] = None  # substring required in stdout
    xfail: Optional[str] = None  # reason this can't pass yet
    skip: Optional[str] = None  # reason this isn't runnable in the harness
    timeout: int = SAMPLE_TIMEOUT_SECONDS  # for samples that legitimately run long


EXPECTATIONS = {
    "hello_activity": Expectation(expect_output="Result: Hello, World!"),
    "hello_activity_async": Expectation(expect_output="Result: Hello, World!"),
    "hello_activity_choice": Expectation(expect_output="Order result:"),
    "hello_activity_heartbeat": Expectation(expect_output="Result: Hello, World!"),
    "hello_activity_method": Expectation(expect_output="Database update executed"),
    "hello_activity_multiprocess": Expectation(
        xfail="multiprocess activity executors (SharedStateManager) unsupported"
    ),
    "hello_activity_retry": Expectation(expect_output="Result: Hello, World!"),
    "hello_async_activity_completion": Expectation(
        xfail="async activity completion is Phase 3"
    ),
    "hello_cancellation": Expectation(
        xfail="its sync activity observes cancellation via heartbeat "
        "(Phase 3); until then the activity thread never exits"
    ),
    "hello_change_log_level": Expectation(
        skip="never exits by design: awaits a workflow whose task fails "
        "forever (identical behavior on a real Temporal server)"
    ),
    "hello_child_workflow": Expectation(expect_output="Result: Hello, World!"),
    "hello_continue_as_new": Expectation(
        # 10 chained runs, each sleeping 1s (plus per-run dispatch latency).
        expect_output="Running workflow iteration 9",
        timeout=90,
    ),
    "hello_cron": Expectation(xfail="cron workflows are Phase 3"),
    "hello_exception": Expectation(),
    "hello_local_activity": Expectation(expect_output="Result: Hello, World!"),
    "hello_mtls": Expectation(skip="requires mTLS certificates and a TLS endpoint"),
    "hello_parallel_activity": Expectation(expect_output="Result:"),
    "hello_patch": Expectation(
        skip="multi-invocation versioning walkthrough; patched() is Phase 4"
    ),
    "hello_query": Expectation(
        xfail="queries a completed workflow: v1 requires RUNNING (README "
        "deviation #2; rehydrate-by-replay is Phase 4)"
    ),
    "hello_search_attributes": Expectation(
        xfail="search-attribute storage + describe() exposure is Phase 3"
    ),
    "hello_signal": Expectation(expect_output="Result:"),
    "hello_update": Expectation(expect_output="Workflow Result:"),
}


@pytest.fixture(scope="session")
def rewritten_samples(tmp_path_factory: pytest.TempPathFactory) -> Path:
    samples_root = ensure_samples() / "hello"
    dest = tmp_path_factory.mktemp("rewritten_hello")
    for name in EXPECTATIONS:
        source = samples_root / f"{name}.py"
        assert source.exists(), (
            f"sample {name} missing from pinned samples-python checkout — "
            "update EXPECTATIONS for the pinned commit"
        )
        rewrite_sample(source, dest)
    return dest


def _params() -> "list[Any]":
    params = []
    for name, exp in sorted(EXPECTATIONS.items()):
        marks = []
        if exp.skip:
            marks.append(pytest.mark.skip(reason=exp.skip))
        elif exp.xfail:
            marks.append(pytest.mark.xfail(reason=exp.xfail, strict=False))
        params.append(pytest.param(name, id=name, marks=marks))
    return params


@pytest.mark.timeout(max(e.timeout for e in EXPECTATIONS.values()) + 30)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("sample_name", _params())
def test_hello_sample(sample_name: str, rewritten_samples: Path) -> None:
    expectation = EXPECTATIONS[sample_name]
    result = subprocess.run(
        [sys.executable, str(RUNNER), str(rewritten_samples / f"{sample_name}.py")],
        capture_output=True,
        text=True,
        timeout=expectation.timeout,
        env={
            **__import__("os").environ,
            "TDB_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
        },
    )
    assert result.returncode == 0, (
        f"sample exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr[-4000:]}"
    )
    if expectation.expect_output:
        # Some samples emit their proof via logging (stderr), not print.
        combined = result.stdout + result.stderr
        assert expectation.expect_output in combined, (
            f"expected {expectation.expect_output!r} in output\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr[-2000:]}"
        )
