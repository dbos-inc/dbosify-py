"""Unit coverage for ``activities.retry_decision``'s non-retryable matching.

The pure decision function is shared by both activity execution paths (local
interpreter + queued ``__temporal_activity`` workflow), so the parity rule —
``non_retryable_error_types`` matches the failure *type* string, never the
structured envelope class — is exercised here without Postgres.
"""

from __future__ import annotations

from dbosify._internal.activities import retry_decision
from dbosify.common import RetryPolicy
from dbosify.exceptions import RetryState


def _decide(policy: RetryPolicy, failure: dict[str, object]):
    return retry_decision(
        policy, attempt=1, failure=failure, elapsed=None, schedule_to_close=None
    )


def test_matches_application_type() -> None:
    policy = RetryPolicy(non_retryable_error_types=["Boom"])
    delay, state = _decide(policy, {"cls": "ApplicationError", "type": "Boom"})
    assert delay is None and state is RetryState.NON_RETRYABLE_FAILURE


def test_matches_coerced_exception_class() -> None:
    # Arbitrary exceptions serialize to an ApplicationError typed by class name.
    policy = RetryPolicy(non_retryable_error_types=["ValueError"])
    delay, _ = _decide(policy, {"cls": "ApplicationError", "type": "ValueError"})
    assert delay is None


def test_ignores_envelope_class() -> None:
    # A structured envelope class with no user-defined ``type`` is never matched
    # against non_retryable_error_types, so it stays retryable.
    policy = RetryPolicy(non_retryable_error_types=["ActivityError"])
    delay, state = _decide(policy, {"cls": "ActivityError", "message": "a"})
    assert delay is not None and state is RetryState.IN_PROGRESS
