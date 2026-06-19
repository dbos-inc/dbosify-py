"""Worker-process activity registries: identity-guarded unregistration and
heartbeat-store hygiene around cancellation.
"""

import pytest

from dbosify import activity
from dbosify.exceptions import CancelledError

KEY = ("unit-run", 7)


def _ctx() -> activity._Context:
    return activity._Context(
        info=activity.Info(), on_heartbeat=lambda *d: None, attempt_key=KEY
    )


def test_unregister_is_identity_guarded() -> None:
    """A cancelled attempt whose unwind outlasts the retry backoff must not
    pop its successor's registration."""
    first, second = _ctx(), _ctx()
    activity._register_attempt(KEY, first)
    activity._register_attempt(KEY, second)  # retry attempt takes over
    activity._unregister_attempt(KEY, first)  # late unwind of attempt 1
    assert activity._live_attempts.get(KEY) is second
    activity._unregister_attempt(KEY, second)
    assert KEY not in activity._live_attempts
    activity._forget_attempt_state(KEY)


def test_cancelled_heartbeat_raises_before_recording() -> None:
    """A post-cancellation heartbeat must not re-populate the cross-attempt
    store the workflow side already cleaned up."""
    ctx = _ctx()
    token = activity._current_context.set(ctx)
    try:
        activity.heartbeat("before")
        assert activity._heartbeat_store[KEY] == ["before"]
        activity._forget_attempt_state(KEY)  # workflow-side cleanup
        ctx.cancelled.set()
        with pytest.raises(CancelledError):
            activity.heartbeat("after")
        assert KEY not in activity._heartbeat_store
    finally:
        activity._current_context.reset(token)
        activity._forget_attempt_state(KEY)
