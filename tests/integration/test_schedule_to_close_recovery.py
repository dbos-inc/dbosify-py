"""SIGKILL-recovery test for schedule-to-close timeouts.

An activity with only ``schedule_to_close`` set hangs forever; the budget times it
out with SCHEDULE_TO_CLOSE. This proves that timeout outcome is durable: a parked
run is crashed after the timeout fires and resumed in a fresh process, where it
must replay the same SCHEDULE_TO_CLOSE outcome from the activity's checkpoint
rather than re-running the activity.
"""

from pathlib import Path
from typing import Dict

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "schedule_to_close_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env() -> Dict[str, str]:
    return {"PYTHONPATH": str(REPO_ROOT)}


@pytest.mark.usefixtures("cleanup_test_databases")
def test_schedule_to_close_timeout_survives_crash() -> None:
    wf_id = "schedule-to-close-recovery"
    first = PythonProcess(WORKER, "start", wf_id, env=_env())
    first.start()
    try:
        # PARKED prints only after the activity timed out and the outcome was
        # recorded, so the timeout is already checkpointed when we crash.
        first.wait_for_line("PARKED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "resume", wf_id, env=_env())
    second.start()
    try:
        line = second.wait_for_line("RESULT ", timeout=60)
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert line.split("RESULT ", 1)[1].strip() == "timed-out:SCHEDULE_TO_CLOSE"
