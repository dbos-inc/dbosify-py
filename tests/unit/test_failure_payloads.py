"""Unit tests for failure serialization (dbosify._internal.payloads).

No database needed. These pin the structural failure envelope — in particular
that an ``ApplicationError``'s severity ``category`` survives the round-trip
(it drives client-side logging/metrics behavior in Temporal).
"""

from dbosify._internal.payloads import deserialize_failure, serialize_failure
from dbosify.exceptions import ApplicationError, ApplicationErrorCategory


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
