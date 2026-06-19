"""Unit tests for ``dbosify.common`` value types.

The RetryPolicy / TypedSearchAttributes / Priority validation tests are adapted
(import-swapped) from temporalio's ``tests/test_common.py``.
"""

import dataclasses
from datetime import timedelta

import pytest

from dbosify.common import (
    Priority,
    RetryPolicy,
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)


def test_priority_default_is_all_unset() -> None:
    assert Priority.default == Priority()
    assert Priority.default.priority_key is None
    assert Priority.default.fairness_key is None
    assert Priority.default.fairness_weight is None


def test_priority_is_frozen() -> None:
    p = Priority(priority_key=2)
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.priority_key = 3  # type: ignore[misc]


def test_priority_validates_priority_key() -> None:
    with pytest.raises(ValueError):
        Priority(priority_key=0)
    with pytest.raises(TypeError):
        Priority(priority_key="high")  # type: ignore[arg-type]
    # A valid positive key is accepted.
    assert Priority(priority_key=1).priority_key == 1


def test_cant_construct_bad_priority() -> None:
    with pytest.raises(TypeError):
        Priority(priority_key=1.1)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Priority(priority_key=-1)


def test_retry_policy_validate() -> None:
    # Validation ignored for max attempts as 1.
    RetryPolicy(initial_interval=timedelta(seconds=-1), maximum_attempts=1)._validate()
    with pytest.raises(ValueError, match="Initial interval cannot be negative"):
        RetryPolicy(initial_interval=timedelta(seconds=-1))._validate()
    with pytest.raises(ValueError, match="Backoff coefficient cannot be less than 1"):
        RetryPolicy(backoff_coefficient=0.5)._validate()
    with pytest.raises(ValueError, match="Maximum interval cannot be negative"):
        RetryPolicy(maximum_interval=timedelta(seconds=-1))._validate()
    with pytest.raises(
        ValueError, match="Maximum interval cannot be less than initial interval"
    ):
        RetryPolicy(
            initial_interval=timedelta(seconds=3), maximum_interval=timedelta(seconds=1)
        )._validate()
    with pytest.raises(ValueError, match="Maximum attempts cannot be negative"):
        RetryPolicy(maximum_attempts=-1)._validate()


def test_typed_search_attribute_duplicates() -> None:
    key1 = SearchAttributeKey.for_keyword("my-key1")
    key2 = SearchAttributeKey.for_int("my-key2")
    key1_dupe = SearchAttributeKey.for_int("my-key1")
    # Different keys fine.
    TypedSearchAttributes(
        [SearchAttributePair(key1, "some-val"), SearchAttributePair(key2, 123)]
    )
    # Same key name bad.
    with pytest.raises(ValueError):
        TypedSearchAttributes(
            [SearchAttributePair(key1, "some-val"), SearchAttributePair(key1_dupe, 123)]
        )


def test_typed_search_attributes_contains_with_falsy_value() -> None:
    int_key = SearchAttributeKey.for_int("my-int")
    attrs = TypedSearchAttributes([SearchAttributePair(int_key, 0)])
    assert int_key in attrs  # type: ignore[comparison-overlap]


def test_typed_search_attributes_contains_with_truthy_value() -> None:
    int_key = SearchAttributeKey.for_int("my-int")
    attrs = TypedSearchAttributes([SearchAttributePair(int_key, 42)])
    assert int_key in attrs  # type: ignore[comparison-overlap]


def test_typed_search_attributes_contains_missing_key() -> None:
    int_key = SearchAttributeKey.for_int("my-int")
    missing_key = SearchAttributeKey.for_keyword("missing")
    attrs = TypedSearchAttributes([SearchAttributePair(int_key, 42)])
    assert missing_key not in attrs  # type: ignore[comparison-overlap]
