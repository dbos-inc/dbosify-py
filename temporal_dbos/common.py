"""Common types and enums, mirroring ``temporalio.common``.

Phase 0 carries only what the interpreter needs (RetryPolicy and the
workflow-ID policy enums); the rest of the module lands with the client
facade in Phase 1.
"""

from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from typing import Optional, Sequence

__all__ = [
    "RetryPolicy",
    "WorkflowIDReusePolicy",
    "WorkflowIDConflictPolicy",
]


@dataclass(frozen=True)
class RetryPolicy:
    """Options for retrying workflows and activities."""

    initial_interval: timedelta = timedelta(seconds=1)
    """Backoff interval for the first retry. Default 1s."""

    backoff_coefficient: float = 2.0
    """Coefficient to multiply previous backoff interval by to get new
    interval. Default 2.0.
    """

    maximum_interval: Optional[timedelta] = None
    """Maximum backoff interval between retries. Default 100x
    :py:attr:`initial_interval`.
    """

    maximum_attempts: int = 0
    """Maximum number of attempts.

    If 0, the default, there is no maximum.
    """

    non_retryable_error_types: Optional[Sequence[str]] = None
    """List of error types that are not retryable."""

    def _validate(self) -> None:
        # Validation taken from the Temporal Go SDK's test suite, mirroring
        # temporalio's RetryPolicy._validate.
        if self.maximum_attempts == 1:
            # Ignore other validation if disabling retries
            return
        if self.initial_interval.total_seconds() < 0:
            raise ValueError("Initial interval cannot be negative")
        if self.backoff_coefficient < 1:
            raise ValueError("Backoff coefficient cannot be less than 1")
        if self.maximum_interval:
            if self.maximum_interval.total_seconds() < 0:
                raise ValueError("Maximum interval cannot be negative")
            if self.maximum_interval < self.initial_interval:
                raise ValueError(
                    "Maximum interval cannot be less than initial interval"
                )
        if self.maximum_attempts < 0:
            raise ValueError("Maximum attempts cannot be negative")


class WorkflowIDReusePolicy(IntEnum):
    """How already-in-use workflow IDs are handled on start."""

    ALLOW_DUPLICATE = 1
    ALLOW_DUPLICATE_FAILED_ONLY = 2
    REJECT_DUPLICATE = 3
    TERMINATE_IF_RUNNING = 4


class WorkflowIDConflictPolicy(IntEnum):
    """How already-running workflows of the same ID are handled on start."""

    UNSPECIFIED = 0
    FAIL = 1
    USE_EXISTING = 2
    TERMINATE_EXISTING = 3
