"""SIGKILL mid scheduled-action (DESIGN §6.7 + recovery is the product).

The kill lands while scheduled actions are in flight (every-second fires with
~2s actions overlap, so several run at once). Recovery must:
  * resume the in-flight actions from their checkpoints, and
  * keep the persisted schedule firing after restart (more occurrences),
with each occurrence's deterministic id ensuring no *twin* execution.

Note on at-least-once (DEVIATIONS failover): an action whose ``record_occurrence``
activity wrote its side-effect but had not yet checkpointed that step when the
worker was killed re-executes that activity on recovery — so an occurrence may
legitimately be recorded twice. The deterministic per-occurrence id still bars
an unbounded twin/replay storm (no id repeats more than once), which is what
this test pins down.
"""

from collections import Counter
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
    counts = Counter(occurrences)
    # The schedule resumed firing after the crash (distinct occurrences beyond
    # the one in flight at kill time).
    assert len(counts) >= 2, occurrences
    # The deterministic per-occurrence id bars a twin / replay storm: no occurrence
    # is recorded more than twice (original write + one at-least-once re-run, DEVIATIONS failover).
    assert max(counts.values()) <= 2, occurrences
