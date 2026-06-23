"""Cross-version PINNED recovery (ARCHITECTURE worker-versioning): the behavioral proof that a
build id (= DBOS application_version) actually pins a workflow to its version.

A workflow stamped with build id ``v1`` is crashed mid-flight. A ``v2`` worker
must NOT recover it (so its buffered release goes unprocessed and it stays
running); a ``v1`` worker then recovers it and completes it. This exercises the
real enforcement — DBOS scoping recovery by application_version — rather than
just checking the recorded version field.
"""

from pathlib import Path

import pytest

from tests.harness import PythonProcess, build_id_env

WORKER = Path(__file__).parent / "version_recovery_worker.py"


@pytest.mark.usefixtures("cleanup_test_databases")
def test_workflow_pinned_to_build_id_across_recovery() -> None:
    wf_id = "version-pin-recovery"

    # 1. A v1 worker starts the workflow (stamped v1); it parks. Crash it.
    first = PythonProcess(WORKER, "start", wf_id, env=build_id_env("v1"))
    first.start()
    try:
        first.wait_for_line("PARKED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    # 2. A v2 worker sends the release and stays up. It must NOT recover the v1
    #    workflow, so the release is never processed and it stays RUNNING.
    idle = PythonProcess(WORKER, "idle", wf_id, env=build_id_env("v2"))
    idle.start()
    try:
        status_line = idle.wait_for_line("STATUS ", timeout=60)
        assert idle.wait() == 0
    finally:
        idle.terminate_and_wait()
    assert (
        "RUNNING" in status_line
    ), f"a v2 worker must not recover the v1 workflow; got {status_line!r}"

    # 3. A v1 worker recovers it, processes the buffered release, and completes.
    resume = PythonProcess(WORKER, "resume", wf_id, env=build_id_env("v1"))
    resume.start()
    try:
        result_line = resume.wait_for_line("RESULT ", timeout=60)
        assert resume.wait() == 0
    finally:
        resume.terminate_and_wait()
    assert result_line.split("RESULT ", 1)[1].strip() == "released"
