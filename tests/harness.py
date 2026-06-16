"""Subprocess harness for kill-and-recover tests.

Recovery correctness is the product: tests launch worker scripts as real OS
processes, SIGKILL them mid-flight, restart them, and assert that workflows
resume correctly. This module provides the process-control plumbing; the
scripts themselves live next to the tests that use them.
"""

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")


async def retry_until_success_async(
    check: Callable[[], Awaitable[T]],
    *,
    interval: float = 0.2,
    max_attempts: int = 100,
) -> T:
    """Poll ``check`` until it returns without raising, then return its value.

    Sleeps with ``asyncio.sleep`` between attempts so the event loop stays free
    to make progress (e.g. an in-process Worker servicing the very thing we are
    waiting on). Prefer this over a fixed ``asyncio.sleep`` in tests: a test
    that waits for a real condition is both faster (returns as soon as the
    condition holds) and more reliable (no guessing the duration). ``check``
    signals "not yet" by raising (e.g. ``assert``).
    """
    error: Optional[BaseException] = None
    for _ in range(max_attempts):
        try:
            return await check()
        except Exception as e:  # noqa: BLE001 — any failure means "not yet"
            error = e
            await asyncio.sleep(interval)
    if error is not None:
        raise error
    raise RuntimeError("retry_until_success_async exhausted without an exception")


class PythonProcess:
    """A Python script running as a subprocess, with line-based stdout matching.

    stdout/stderr are merged and captured by a background reader thread, so
    `wait_for_line` can enforce timeouts even when the process is silent.
    """

    def __init__(
        self,
        script: Path,
        *args: str,
        env: Optional[dict[str, str]] = None,
    ) -> None:
        self.script = script
        self.args = args
        self.env = {**os.environ, **(env or {})}
        self.lines: list[str] = []
        # Full output history; unlike `lines`, never consumed by wait_for_line.
        self.transcript: list[str] = []
        self._proc: Optional[subprocess.Popen[str]] = None
        self._cond = threading.Condition()
        self._reader: Optional[threading.Thread] = None

    def start(self) -> None:
        assert self._proc is None, "process already started"
        self._proc = subprocess.Popen(
            [sys.executable, "-u", str(self.script), *self.args],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=self.env,
        )
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        for line in self._proc.stdout:
            with self._cond:
                self.lines.append(line)
                self.transcript.append(line)
                self._cond.notify_all()

    def wait_for_line(self, needle: str, timeout: float = 30.0) -> str:
        """Block until a captured line contains `needle`; return that line.

        Each line is consumed by at most one successful `wait_for_line` call,
        so two waits for the same needle require two occurrences.
        """
        deadline = time.monotonic() + timeout
        scanned = 0
        with self._cond:
            while True:
                while scanned < len(self.lines):
                    line = self.lines[scanned]
                    scanned += 1
                    if needle in line:
                        del self.lines[:scanned]
                        return line
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"timed out waiting for {needle!r} from {self.script.name}; "
                        f"output so far:\n{''.join(self.lines)}"
                    )
                self._cond.wait(timeout=remaining)

    def sigkill(self) -> None:
        """SIGKILL the process — no cleanup handlers run, like a real crash."""
        assert self._proc is not None, "process not started"
        self._proc.send_signal(signal.SIGKILL)

    def sigabrt_for_stacks(self, grace_seconds: float = 3.0) -> None:
        """SIGABRT the process to collect a faulthandler all-threads stack
        dump in the transcript (requires PYTHONFAULTHANDLER=1 in its env),
        then give the reader thread a moment to capture it. Diagnostic-only:
        the process is dead afterwards."""
        if self._proc is None or self._proc.poll() is not None:
            return
        self._proc.send_signal(signal.SIGABRT)
        try:
            self._proc.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            self._proc.kill()
        if self._reader is not None:
            self._reader.join(timeout=grace_seconds)

    def wait(self, timeout: float = 30.0) -> int:
        """Wait for exit and return the return code (-9 after a SIGKILL)."""
        assert self._proc is not None, "process not started"
        returncode = self._proc.wait(timeout=timeout)
        if self._reader is not None:
            self._reader.join(timeout=5.0)
        return returncode

    def terminate_and_wait(self, timeout: float = 30.0) -> None:
        """Best-effort cleanup for test teardown paths."""
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=timeout)
