"""Conformance: the samples-python ``schedules/`` corpus — part of the Phase 3
exit gate (DESIGN §9).

Unlike ``hello/`` and ``message_passing/``, this corpus is a long-running
worker (``run_worker.py``) plus a series of independent operation scripts
(``start_schedule.py``, ``describe_schedule.py``, ...) that each connect as a
client and perform one schedule operation. The harness rewrites the whole
flat directory (so sibling imports like ``from your_workflows import ...``
resolve), launches the worker via the runner, then runs each operation script
in dependency order and asserts on exit codes and the documented output.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conformance.samples import ensure_samples, rewrite_sample
from tests.dbconfig import system_database_url

RUNNER = Path(__file__).parent / "runner.py"
SCRIPT_TIMEOUT_SECONDS = 60

# Operation scripts in the order they must run (start first, delete last).
# Each tuple is (module, required_stdout_substring | None).
OPERATIONS = [
    ("start_schedule", None),
    ("describe_schedule", "Returns the note: Here's a note on my Schedule."),
    ("list_schedule", "List Schedule Info:"),
    ("trigger_schedule", None),
    ("update_schedule", None),
    ("pause_schedule", None),
    ("backfill_schedule", None),
    ("delete_schedule", None),
]


@pytest.fixture(scope="session")
def rewritten_schedules(tmp_path_factory: pytest.TempPathFactory) -> Path:
    samples_root = ensure_samples()
    source_dir = samples_root / "schedules"
    dest = tmp_path_factory.mktemp("rewritten_schedules")
    for source in source_dir.glob("*.py"):
        rewrite_sample(source, dest)
    return dest


@pytest.mark.timeout(SCRIPT_TIMEOUT_SECONDS * len(OPERATIONS) + 120)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_schedules_corpus(rewritten_schedules: Path) -> None:
    from tests.harness import PythonProcess

    env = {
        "PYTHONPATH": f"{rewritten_schedules}{os.pathsep}{Path(__file__).parents[2]}",
        "TDB_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
    }

    worker = PythonProcess(RUNNER, "run_worker", env=env)
    worker.start()
    try:
        worker.wait_for_line("DBOS launched", timeout=60)
        for module, expect in OPERATIONS:
            result = subprocess.run(
                [sys.executable, str(RUNNER), module],
                capture_output=True,
                text=True,
                timeout=SCRIPT_TIMEOUT_SECONDS,
                env={**os.environ, **env},
            )
            assert result.returncode == 0, (
                f"schedules/{module}.py exited {result.returncode}\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{result.stderr[-4000:]}\n"
                f"--- worker tail ---\n{''.join(worker.transcript[-30:])}"
            )
            if expect is not None:
                assert expect in result.stdout, (
                    f"expected {expect!r} in schedules/{module}.py output\n"
                    f"--- stdout ---\n{result.stdout}"
                )
    finally:
        worker.terminate_and_wait()
