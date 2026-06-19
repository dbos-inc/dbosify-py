"""Decoration-time behavior of dynamic handlers/activities and handler
descriptions (DESIGN §6.1/§6.8). End-to-end dispatch lives in
tests/integration/test_dynamic_handlers.py; dynamic *workflows* are rejected
(DEVIATIONS dynamic-handlers)."""

import collections.abc
import typing
from typing import Sequence

import pytest

from dbosify import activity, workflow
from dbosify._internal import registry
from dbosify.common import RawValue


def test_dynamic_workflow_rejected() -> None:
    with pytest.raises(NotImplementedError, match="dynamic workflows"):

        @workflow.defn(dynamic=True)
        class _W:
            @workflow.run
            async def run(self, args: Sequence[RawValue]) -> None: ...


def test_dynamic_handlers_stored_under_none_key() -> None:
    @workflow.defn
    class W:
        @workflow.signal(dynamic=True, description="any signal")
        def any_signal(self, name: str, args: Sequence[RawValue]) -> None: ...

        @workflow.query(dynamic=True, description="any query")
        def any_query(self, name: str, args: Sequence[RawValue]) -> str:
            return name

        @workflow.update(dynamic=True, description="any update")
        def any_update(self, name: str, args: Sequence[RawValue]) -> str:
            return name

        @workflow.run
        async def run(self) -> None: ...

    defn = registry.workflow_definition_of(W)
    for table, desc in (
        (defn.signals, "any signal"),
        (defn.queries, "any query"),
        (defn.updates, "any update"),
    ):
        assert None in table
        assert table[None].name is None
        assert table[None].description == desc


def test_named_handler_description_captured() -> None:
    @workflow.defn
    class W:
        @workflow.signal(description="sig")
        def s(self, x: int) -> None: ...

        @workflow.query(description="qry")
        def q(self) -> int:
            return 0

        @workflow.update(description="upd")
        def u(self, x: int) -> int:
            return x

        @workflow.run
        async def run(self) -> None: ...

    defn = registry.workflow_definition_of(W)
    assert defn.signals["s"].description == "sig"
    assert defn.queries["q"].description == "qry"
    assert defn.updates["u"].description == "upd"


@pytest.mark.parametrize("kind", ["signal", "query", "update"])
def test_name_and_dynamic_mutually_exclusive(kind: str) -> None:
    decorator = getattr(workflow, kind)
    with pytest.raises(RuntimeError, match="name and dynamic"):
        decorator(name="x", dynamic=True)


def test_dynamic_signal_wrong_signature_rejected() -> None:
    with pytest.raises(RuntimeError, match="Dynamic signal handler"):

        @workflow.defn
        class _W:
            @workflow.signal(dynamic=True)
            def bad(self, name: str) -> None: ...  # missing Sequence[RawValue]

            @workflow.run
            async def run(self) -> None: ...


def test_dynamic_query_wrong_signature_rejected() -> None:
    with pytest.raises(RuntimeError, match="Dynamic query handler"):

        @workflow.defn
        class _W:
            @workflow.query(dynamic=True)
            def bad(self, name: str, extra: int) -> str:  # wrong 2nd type
                return name

            @workflow.run
            async def run(self) -> None: ...


def test_multiple_dynamic_handlers_rejected() -> None:
    with pytest.raises(ValueError, match="Multiple dynamic signal"):

        @workflow.defn
        class _W:
            @workflow.signal(dynamic=True)
            def s1(self, name: str, args: Sequence[RawValue]) -> None: ...

            @workflow.signal(dynamic=True)
            def s2(self, name: str, args: Sequence[RawValue]) -> None: ...

            @workflow.run
            async def run(self) -> None: ...


def test_dynamic_activity_defn() -> None:
    @activity.defn(dynamic=True)
    async def any_activity(args: Sequence[RawValue]) -> str:
        return "ok"

    defn = registry.activity_definition_of(any_activity)
    assert defn.dynamic is True


def test_dynamic_activity_wrong_signature_rejected() -> None:
    with pytest.raises(TypeError, match="Dynamic activity"):

        @activity.defn(dynamic=True)
        def bad(x: int) -> str:  # not a single Sequence[RawValue]
            return str(x)


def test_duplicate_named_activity_rejected() -> None:
    # Mirrors temporalio: registering two activities under one name raises.
    @activity.defn(name="dup")
    async def first() -> None: ...

    @activity.defn(name="dup")
    async def second() -> None: ...

    registry._activities.clear()
    try:
        registry.register_activity(registry.activity_definition_of(first))
        with pytest.raises(ValueError, match="More than one activity named dup"):
            registry.register_activity(registry.activity_definition_of(second))
    finally:
        registry._activities.clear()


def test_duplicate_dynamic_activity_rejected() -> None:
    # Mirrors temporalio: a second dynamic (catch-all) activity raises.
    @activity.defn(dynamic=True)
    async def dyn1(args: Sequence[RawValue]) -> str:
        return "1"

    @activity.defn(dynamic=True)
    async def dyn2(args: Sequence[RawValue]) -> str:
        return "2"

    registry._dynamic_activity = None
    try:
        registry.register_activity(registry.activity_definition_of(dyn1))
        with pytest.raises(TypeError, match="More than one dynamic activity"):
            registry.register_activity(registry.activity_definition_of(dyn2))
    finally:
        registry._dynamic_activity = None


def test_dynamic_activity_name_mutually_exclusive() -> None:
    with pytest.raises(RuntimeError, match="name and dynamic"):
        activity.defn(name="x", dynamic=True)


def test_payload_converter_exposed() -> None:
    from dbosify.converter import PayloadConverter

    assert isinstance(workflow.payload_converter(), PayloadConverter)
    assert isinstance(activity.payload_converter(), PayloadConverter)


def test_dynamic_handler_accepts_both_sequence_spellings() -> None:
    # temporalio accepts the dynamic-handler arg typed as either typing.Sequence
    # or collections.abc.Sequence of RawValue (not == each other); both must validate.
    for seq in (typing.Sequence[RawValue], collections.abc.Sequence[RawValue]):

        @workflow.defn
        class _W:
            @workflow.signal(dynamic=True)
            def s(self, name: str, args: seq) -> None: ...  # type: ignore[valid-type]

            @workflow.run
            async def run(self) -> None: ...

        defn = registry.workflow_definition_of(_W)
        assert None in defn.signals


def test_dynamic_activity_accepts_both_sequence_spellings() -> None:
    for seq in (typing.Sequence[RawValue], collections.abc.Sequence[RawValue]):

        @activity.defn(dynamic=True)
        def _a(args: seq) -> str:  # type: ignore[valid-type]
            return "ok"

        assert registry.activity_definition_of(_a).dynamic is True


def test_dynamic_handler_rejects_wrong_sequence_element() -> None:
    # Sequence of the wrong element type, or a non-Sequence container, is
    # rejected: the widened Sequence spelling covers the container, not the element.
    with pytest.raises(RuntimeError, match="Dynamic signal handler"):

        @workflow.defn
        class _W:
            @workflow.signal(dynamic=True)
            def s(self, name: str, args: typing.List[RawValue]) -> None: ...

            @workflow.run
            async def run(self) -> None: ...

    with pytest.raises(TypeError, match="Dynamic activity"):

        @activity.defn(dynamic=True)
        def _a(args: Sequence[int]) -> str:
            return "x"


def test_no_thread_cancel_exception_default_and_true_ok() -> None:
    # Our only mode is cooperative (== no_thread_cancel_exception=True), so the
    # default and an explicit True both decorate cleanly.
    @activity.defn
    def a() -> None: ...

    @activity.defn(no_thread_cancel_exception=True)
    def b() -> None: ...

    assert registry.activity_definition_of(a).name == "a"
    assert registry.activity_definition_of(b).name == "b"


def test_no_thread_cancel_exception_false_rejected() -> None:
    # Asking for Temporal's raise-into-the-thread behavior fails loudly at
    # decoration time rather than silently degrading (DEVIATIONS sync-activity-cancel).
    with pytest.raises(NotImplementedError, match="no_thread_cancel_exception"):

        @activity.defn(no_thread_cancel_exception=False)
        def _a() -> None: ...
