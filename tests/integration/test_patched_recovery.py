"""SIGKILL-recovery tests for workflow.patched() (DESIGN §6.8) — the replay
semantics that the in-process suite (test_patched.py) can't reach.

Two scenarios, both crashing a parked run mid-flight and resuming a fresh
process:

1. Old in-flight run, redeployed with patched() code: a v1 run (no patch)
   crashes; the resume process runs freshly-deployed v2 code. With no marker in
   history, patched() returns False and the run replays the OLDER path — proving
   a False patch claims no checkpoint position, so a pre-patch history still
   replays deterministically.

2. New run: a v2 run records the marker and crashes; on resume patched() finds
   the marker and replays the NEWER path.
"""

from pathlib import Path
from typing import Dict

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "patched_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(version: str) -> Dict[str, str]:
    return {"PYTHONPATH": str(REPO_ROOT), "PATCH_VERSION": version}


def _crash_then_resume(wf_id: str, *, start_version: str, resume_version: str) -> str:
    first = PythonProcess(WORKER, "start", wf_id, env=_env(start_version))
    first.start()
    try:
        first.wait_for_line("PARKED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "resume", wf_id, env=_env(resume_version))
    second.start()
    try:
        line = second.wait_for_line("RESULT ", timeout=60)
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()
    return line.split("RESULT ", 1)[1].strip()


@pytest.mark.usefixtures("cleanup_test_databases")
def test_old_inflight_run_takes_old_path_after_redeploy() -> None:
    # v1 (no patch) crashes mid-flight; v2 (with patch) is deployed for recovery.
    # The marker is absent, so patched() returns False → older path replays.
    branch = _crash_then_resume(
        "patched-old-inflight", start_version="v1", resume_version="v2"
    )
    assert branch == "old"


@pytest.mark.usefixtures("cleanup_test_databases")
def test_new_run_replays_newer_path() -> None:
    # A fresh v2 run records the marker before crashing; recovery finds it and
    # replays the newer path.
    branch = _crash_then_resume(
        "patched-new-run", start_version="v2", resume_version="v2"
    )
    assert branch == "new"
