"""Unit tests for the memo/search-attribute codec (no Postgres) — the fixes
from the PR review: temporalio-faithful type guessing, datetime validation,
malformed-entry tolerance on decode, and the deprecated-form warning."""

import warnings
from datetime import datetime, timezone

import pytest

from dbosify._internal import attributes as A
from dbosify.common import (
    SearchAttributeIndexedValueType,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
    _warn_on_deprecated_search_attributes,
)


def test_untyped_bool_guesses_int_like_temporalio() -> None:
    # bool is an int subclass and temporalio checks int before bool, so an
    # untyped boolean value guesses to Int (the for_bool branch is dead). We
    # must match that, not "improve" it.
    key = SearchAttributeKey._guess_from_untyped_values("flag", [True])
    assert key is not None
    assert key.indexed_value_type == SearchAttributeIndexedValueType.INT
    encoded = A.encode_search_attributes({"flag": [True]})
    assert encoded["flag"]["t"] == "Int"


def test_naive_datetime_rejected_tzaware_ok() -> None:
    key = SearchAttributeKey.for_datetime("When")
    naive = TypedSearchAttributes([SearchAttributePair(key, datetime(2026, 6, 16))])
    with pytest.raises(ValueError, match="timezone-aware"):
        A.encode_search_attributes(naive)
    aware = TypedSearchAttributes(
        [SearchAttributePair(key, datetime(2026, 6, 16, tzinfo=timezone.utc))]
    )
    assert A.encode_search_attributes(aware)["When"]["t"] == "Datetime"


def test_non_datetime_for_datetime_key_rejected() -> None:
    key = SearchAttributeKey.for_datetime("When")
    bad = TypedSearchAttributes([SearchAttributePair(key, "not-a-datetime")])
    with pytest.raises(TypeError):
        A.encode_search_attributes(bad)


def test_decode_skips_malformed_entries() -> None:
    stored = {
        "good": {"t": "Keyword", "v": "x"},
        "missing_t": {"v": "x"},
        "missing_v": {"t": "Keyword"},
        "not_a_dict": "scalar",
        "unknown_type": {"t": "Nonsense", "v": 1},
    }
    typed = A.decode_search_attributes(stored)
    names = {p.key.name for p in typed}
    assert names == {"good"}
    assert typed[SearchAttributeKey.for_keyword("good")] == "x"


def test_decode_skips_unparseable_datetime() -> None:
    # A Datetime-typed entry with a non-ISO value must not crash the decode.
    typed = A.decode_search_attributes(
        {"ok": {"t": "Keyword", "v": "y"}, "bad_dt": {"t": "Datetime", "v": "nope"}}
    )
    assert {p.key.name for p in typed} == {"ok"}


def test_deprecation_warning_helper() -> None:
    with pytest.warns(DeprecationWarning):
        _warn_on_deprecated_search_attributes({"CustomKeyword": ["x"]})
    # Typed form and None must NOT warn.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _warn_on_deprecated_search_attributes(None)
        _warn_on_deprecated_search_attributes(
            TypedSearchAttributes(
                [SearchAttributePair(SearchAttributeKey.for_keyword("k"), "v")]
            )
        )
