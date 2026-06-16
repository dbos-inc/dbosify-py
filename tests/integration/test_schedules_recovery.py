"""SIGKILL mid scheduled-action (DESIGN §6.7 + recovery is the product).

The kill lands while a scheduled action is in flight — after its recording
activity checkpointed but before the run closed. Recovery must:
  * resume the in-flight action from its checkpoints (the activity executes
    exactly once — no duplicate occurrence line), and
  * keep the persisted schedule firing after restart (more occurrences),
with each occurrence's deterministic id ensuring no double execution.
"""

from pathlib import Path

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "schedules_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_scheduled_action(tmp_path: Path) -> None:
    effects = tmp_path / "effects"

    first = PythonProcess(WORKER, "start", str(effects), env=ENV)
    first.start()
    try:
        # An action has recorded its occurrence and is now mid-sleep; its run
        # has not closed. Kill here.
        first.wait_for_line("ACTION_FIRED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "resume", str(effects), env=ENV)
    second.start()
    try:
        second.wait_for_line("DONE", timeout=60)
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    occurrences = effects.read_text().splitlines()
    # The schedule resumed firing after the crash (beyond the one in flight at
    # kill time).
    assert len(occurrences) >= 2
    # Each occurrence recorded exactly once: the action in flight at the kill
    # came back from its checkpoint (its activity was not re-executed), and the
    # deterministic per-occurrence id kept any replayed fire from starting a
    # twin.
    assert len(occurrences) == len(set(occurrences)), occurrences
