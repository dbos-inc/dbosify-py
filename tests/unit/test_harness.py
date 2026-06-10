"""The kill-and-recover harness must itself work before Phase 0 leans on it."""

from pathlib import Path

from tests.harness import PythonProcess

# Simulates a crash-and-recover worker: first run marks itself started and
# hangs; a restarted run finds the marker and completes.
VICTIM_SCRIPT = """\
import pathlib
import sys
import time

state = pathlib.Path(sys.argv[1])
if state.exists():
    print("RESUMED", flush=True)
    sys.exit(0)
state.write_text("started")
print("READY", flush=True)
time.sleep(60)
"""


def test_sigkill_and_restart(tmp_path: Path) -> None:
    script = tmp_path / "victim.py"
    script.write_text(VICTIM_SCRIPT)
    state = tmp_path / "state"

    first = PythonProcess(script, str(state))
    first.start()
    try:
        first.wait_for_line("READY")
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(script, str(state))
    second.start()
    try:
        second.wait_for_line("RESUMED")
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()


def test_wait_for_line_timeout(tmp_path: Path) -> None:
    script = tmp_path / "victim.py"
    script.write_text(VICTIM_SCRIPT)

    proc = PythonProcess(script, str(tmp_path / "state"))
    proc.start()
    try:
        proc.wait_for_line("READY")
        try:
            proc.wait_for_line("NEVER_PRINTED", timeout=0.5)
        except TimeoutError:
            pass
        else:
            raise AssertionError("expected TimeoutError")
    finally:
        proc.terminate_and_wait()
