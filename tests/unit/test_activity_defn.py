"""Decoration- and registration-time validation of ``@activity.defn``,
adapted from temporalio's ``tests/worker/test_activity.py``.
"""

import pytest

from dbosify import activity
from dbosify._internal import registry


def test_activity_kwonly_params() -> None:
    # Activities are invoked positionally, so keyword-only params are rejected
    # at decoration time (temporalio parity).
    with pytest.raises(TypeError) as err:

        @activity.defn
        async def say_hello(*, name: str) -> str:
            return f"Hello, {name}!"

    assert str(err.value).endswith("cannot have keyword-only arguments")


def test_activity_var_keyword_is_allowed() -> None:
    # **kwargs is VAR_KEYWORD, not keyword-only — allowed, as in temporalio.
    @activity.defn
    async def takes_kwargs(name: str, **rest: object) -> str:
        return name

    assert registry.activity_definition_of(takes_kwargs).name == "takes_kwargs"


def test_activity_without_decorator() -> None:
    async def say_hello(name: str) -> str:
        return f"Hello, {name}!"

    with pytest.raises(TypeError, match="missing the @activity.defn decorator"):
        registry.activity_definition_of(say_hello)
