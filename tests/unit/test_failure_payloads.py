"""Unit tests for failure serialization (dbosify._internal.payloads).

No database needed. These pin the structural failure envelope — in particular
that an ``ApplicationError``'s severity ``category`` survives the round-trip
(it drives client-side logging/metrics behavior in Temporal).
"""

from dbosify._internal.payloads import deserialize_failure, serialize_failure
from dbosify.exceptions import ApplicationError, ApplicationErrorCategory, ServerError


def test_application_error_category_roundtrips() -> None:
    exc = ApplicationError(
        "benign boom", type="MyErr", category=ApplicationErrorCategory.BENIGN
    )
    back = deserialize_failure(serialize_failure(exc))
    assert isinstance(back, ApplicationError)
    assert back.category == ApplicationErrorCategory.BENIGN
    assert back.type == "MyErr"


def test_application_error_default_category_is_unspecified() -> None:
    back = deserialize_failure(serialize_failure(ApplicationError("plain")))
    assert isinstance(back, ApplicationError)
    assert back.category == ApplicationErrorCategory.UNSPECIFIED


def test_non_failure_exception_defaults_to_unspecified_category() -> None:
    # A builtin exception goes through the catch-all (no category key in the
    # envelope); it must still deserialize with a defined category.
    back = deserialize_failure(serialize_failure(ValueError("oops")))
    assert isinstance(back, ApplicationError)
    assert back.category == ApplicationErrorCategory.UNSPECIFIED


def test_implicit_context_chain_is_recorded_as_cause() -> None:
    # `except X: raise Y` (no `from`) chains the original via __context__;
    # temporalio records it as the cause, so we must too.
    try:
        try:
            raise ValueError("root cause")
        except ValueError:
            raise ApplicationError("wrapped", type="Wrapped")
    except ApplicationError as exc:
        back = deserialize_failure(serialize_failure(exc))
    assert isinstance(back.__cause__, ApplicationError)
    assert back.__cause__.type == "ValueError"
    assert "root cause" in str(back.__cause__)


def test_suppressed_context_drops_cause() -> None:
    # `raise Y from None` suppresses the implicit chain.
    try:
        try:
            raise ValueError("root cause")
        except ValueError:
            raise ApplicationError("wrapped") from None
    except ApplicationError as exc:
        back = deserialize_failure(serialize_failure(exc))
    assert back.__cause__ is None


def test_server_error_roundtrips_preserving_non_retryable() -> None:
    back = deserialize_failure(
        serialize_failure(ServerError("srv", non_retryable=True))
    )
    assert isinstance(back, ServerError)
    assert back.non_retryable is True
    assert back.message == "srv"
    # The default must round-trip as retryable, not silently flip.
    back2 = deserialize_failure(serialize_failure(ServerError("srv2")))
    assert isinstance(back2, ServerError)
    assert back2.non_retryable is False
