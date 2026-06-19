"""Unit coverage (no database) for the chain-hop building blocks: the input
meta-envelope, cron-expression helpers, and the workflow retry-delay
decision.
"""

from datetime import datetime, timedelta, timezone

import pytest

from dbosify._internal import schedules
from dbosify._internal.dispatcher import _workflow_retry_delay
from dbosify._internal.payloads import (
    RunMeta,
    deserialize_retry_policy,
    serialize_retry_policy,
    unwrap_input,
    wrap_input,
)
from dbosify.common import RetryPolicy


class TestInputEnvelope:
    def test_plain_args_pass_through_unwrapped(self) -> None:
        # The pre-envelope checkpoint format: a bare args list. Must stay
        # byte-identical so existing executions recover.
        assert wrap_input([1, "a"]) == [1, "a"]
        assert wrap_input([1, "a"], RunMeta()) == [1, "a"]

    def test_unwrap_plain_list(self) -> None:
        args, meta = unwrap_input([1, "a"])
        assert args == [1, "a"]
        assert meta.is_empty()
        assert meta.attempt == 1

    def test_meta_round_trip(self) -> None:
        meta = RunMeta(
            cron="* * * * *",
            attempt=3,
            retry_policy={"initial_interval": 1.0},
            last_completion={"value": None},
            last_failure={"cls": "ApplicationError", "message": "x"},
        )
        args, out = unwrap_input(wrap_input(["a"], meta))
        assert args == ["a"]
        assert out == meta

    def test_carried_forward_resets_attempt_keeps_config(self) -> None:
        meta = RunMeta(
            cron="@hourly",
            attempt=7,
            retry_policy={"initial_interval": 1.0},
            run_timeout=30.0,
            last_completion={"value": 41},
            last_failure={"cls": "ApplicationError", "message": "x"},
        )
        carried = meta.carried_forward()
        assert carried.attempt == 1
        assert carried.cron == meta.cron
        assert carried.retry_policy == meta.retry_policy
        assert carried.run_timeout == meta.run_timeout
        assert carried.last_completion == meta.last_completion
        assert carried.last_failure == meta.last_failure

    def test_run_timeout_rides_the_envelope(self) -> None:
        args, meta = unwrap_input(wrap_input([], RunMeta(run_timeout=2.5)))
        assert meta.run_timeout == 2.5
        assert not meta.is_empty()

    def test_retry_policy_round_trip(self) -> None:
        policy = RetryPolicy(
            initial_interval=timedelta(milliseconds=250),
            backoff_coefficient=1.5,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=4,
            non_retryable_error_types=["Boom"],
        )
        assert deserialize_retry_policy(serialize_retry_policy(policy)) == policy

    def test_retry_policy_round_trip_defaults(self) -> None:
        policy = RetryPolicy()
        assert deserialize_retry_policy(serialize_retry_policy(policy)) == policy


class TestCronHelpers:
    def test_five_field_next_fire(self) -> None:
        now = datetime(2026, 6, 12, 10, 30, 25, tzinfo=timezone.utc)
        assert schedules.next_fire_delay("* * * * *", now) == 35.0
        assert schedules.next_fire_delay("*/2 * * * *", now) == 95.0

    def test_six_field_seconds_extension(self) -> None:
        now = datetime(2026, 6, 12, 10, 30, 25, 500000, tzinfo=timezone.utc)
        assert schedules.next_fire_delay("* * * * * *", now) == 0.5

    def test_cron_tz_prefix(self) -> None:
        # 10:30 UTC == 06:30 in New York (June, EDT): firing "daily at 07:00
        # New York" is half an hour out.
        now = datetime(2026, 6, 12, 10, 30, 0, tzinfo=timezone.utc)
        delay = schedules.next_fire_delay("CRON_TZ=America/New_York 0 7 * * *", now)
        assert delay == 1800.0

    def test_naive_now_treated_as_utc(self) -> None:
        aware = datetime(2026, 6, 12, 10, 30, 25, tzinfo=timezone.utc)
        naive = datetime(2026, 6, 12, 10, 30, 25)
        assert schedules.next_fire_delay("* * * * *", naive) == (
            schedules.next_fire_delay("* * * * *", aware)
        )

    @pytest.mark.parametrize(
        "expr",
        ["", "bogus", "61 * * * *", "CRON_TZ=Not/AZone * * * * *"],
    )
    def test_invalid_expressions_rejected(self, expr: str) -> None:
        with pytest.raises(ValueError):
            schedules.validate_cron(expr)


def _failure(**overrides: object) -> "dict[str, object]":
    return {"cls": "ApplicationError", "message": "boom", "type": None, **overrides}


class TestWorkflowRetryDelay:
    POLICY = serialize_retry_policy(
        RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_attempts=0,
        )
    )

    def test_exponential_backoff_series(self) -> None:
        delays = [
            _workflow_retry_delay(self.POLICY, attempt, _failure())
            for attempt in (1, 2, 3, 4)
        ]
        assert delays == [1.0, 2.0, 4.0, 8.0]

    def test_default_maximum_interval_caps_at_100x_initial(self) -> None:
        assert _workflow_retry_delay(self.POLICY, 20, _failure()) == 100.0

    def test_explicit_maximum_interval(self) -> None:
        policy = serialize_retry_policy(
            RetryPolicy(maximum_interval=timedelta(seconds=3))
        )
        assert _workflow_retry_delay(policy, 10, _failure()) == 3.0

    def test_maximum_attempts_exhausted(self) -> None:
        policy = serialize_retry_policy(RetryPolicy(maximum_attempts=3))
        assert _workflow_retry_delay(policy, 2, _failure()) is not None
        assert _workflow_retry_delay(policy, 3, _failure()) is None

    def test_non_retryable_flag(self) -> None:
        assert (
            _workflow_retry_delay(self.POLICY, 1, _failure(non_retryable=True)) is None
        )

    def test_non_retryable_error_types_match_application_type(self) -> None:
        policy = serialize_retry_policy(RetryPolicy(non_retryable_error_types=["Boom"]))
        assert _workflow_retry_delay(policy, 1, _failure(type="Boom")) is None
        assert _workflow_retry_delay(policy, 1, _failure(type="Other")) == 1.0

    def test_non_retryable_error_types_fall_back_to_envelope_class(self) -> None:
        policy = serialize_retry_policy(
            RetryPolicy(non_retryable_error_types=["ActivityError"])
        )
        failure = {"cls": "ActivityError", "message": "a"}
        assert _workflow_retry_delay(policy, 1, failure) is None

    def test_next_retry_delay_override(self) -> None:
        assert (
            _workflow_retry_delay(self.POLICY, 1, _failure(next_retry_delay=42.5))
            == 42.5
        )

    def test_cancelled_and_terminated_failures_never_retry(self) -> None:
        # Temporal's isRetryable: cancellation/termination failures end the
        # retry chain regardless of policy.
        for cls_name in ("CancelledError", "TerminatedError"):
            failure = {"cls": cls_name, "message": "x"}
            assert _workflow_retry_delay(self.POLICY, 1, failure) is None

    def test_timeout_failures_retry_only_start_to_close_and_heartbeat(self) -> None:
        # TimeoutType: 1=START_TO_CLOSE, 2=SCHEDULE_TO_START,
        # 3=SCHEDULE_TO_CLOSE, 4=HEARTBEAT (Temporal retries only 1 and 4).
        for timeout_type, expected in [(1, 1.0), (2, None), (3, None), (4, 1.0)]:
            failure = {
                "cls": "TimeoutError",
                "message": "x",
                "timeout_type": timeout_type,
            }
            assert _workflow_retry_delay(self.POLICY, 1, failure) == expected
