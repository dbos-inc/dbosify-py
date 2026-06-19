"""Unit tests for ``dbosify.common`` value types."""

import dataclasses

import pytest

from dbosify.common import Priority


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
