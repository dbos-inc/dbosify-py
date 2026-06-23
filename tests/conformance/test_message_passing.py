"""Conformance: the samples-python ``message_passing/`` corpus.

Unlike ``hello/``, these are multi-file packages run as two processes: a
worker (runs until interrupted) and a starter (drives the workflow and
prints results). The harness rewrites the whole package tree, launches the
worker via the runner in module mode, waits for its ready line, runs the
starter to completion, and asserts on the starter's output.
"""

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import pytest

from tests.conformance.samples import ensure_samples, rewrite_package
from tests.dbconfig import system_database_url

RUNNER = Path(__file__).parent / "runner.py"
STARTER_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class Sample:
    package: str  # dotted package under samples-python
    expect_output: Optional[str] = None  # substring required in starter stdout
    ready_line: str = "orker started"  # worker readiness marker (log line)
    xfail: Optional[str] = None
    skip: Optional[str] = None


SAMPLES = {
    "introduction": Sample(
        package="message_passing.introduction",
        expect_output="language changed: ENGLISH -> CHINESE",
    ),
    "safe_message_handlers": Sample(
        package="message_passing.safe_message_handlers",
        expect_output="Cluster shut down successfully",
    ),
    "waiting_for_handlers": Sample(
        package="message_passing.waiting_for_handlers",
        expect_output="caller received workflow result",
    ),
    "waiting_for_handlers_and_compensation": Sample(
        package="message_passing.waiting_for_handlers_and_compensation",
        expect_output="caller received workflow result",
    ),
    "update_with_start_lazy_init": Sample(
        package="message_passing.update_with_start.lazy_initialization",
        expect_output="final order:",
    ),
}


@pytest.fixture(scope="session")
def rewritten_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    samples_root = ensure_samples()
    dest = tmp_path_factory.mktemp("rewritten_message_passing")
    for sample in SAMPLES.values():
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


@pytest.mark.timeout(STARTER_TIMEOUT_SECONDS + 90)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("sample_name", _params())
def test_message_passing_sample(sample_name: str, rewritten_root: Path) -> None:
    from tests.harness import PythonProcess

    sample = SAMPLES[sample_name]
    env = {
        "PYTHONPATH": f"{rewritten_root}{os.pathsep}{Path(__file__).parents[2]}",
        "DBOSIFY_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
    }

    worker = PythonProcess(RUNNER, f"{sample.package}.worker", env=env)
    worker.start()
    try:
        worker.wait_for_line(sample.ready_line, timeout=60)
        starter = subprocess.run(
            [sys.executable, str(RUNNER), f"{sample.package}.starter"],
            capture_output=True,
            text=True,
            timeout=STARTER_TIMEOUT_SECONDS,
            env={**os.environ, **env},
        )
        assert starter.returncode == 0, (
            f"starter exited {starter.returncode}\n"
            f"--- stdout ---\n{starter.stdout}\n"
            f"--- stderr ---\n{starter.stderr[-4000:]}\n"
            f"--- worker tail ---\n{''.join(worker.transcript[-30:])}"
        )
        if sample.expect_output:
            assert sample.expect_output in starter.stdout, (
                f"expected {sample.expect_output!r} in starter output\n"
                f"--- stdout ---\n{starter.stdout}"
            )
    finally:
        worker.terminate_and_wait()
