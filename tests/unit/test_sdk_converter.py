"""Converter unit tests adapted from temporalio's ``tests/test_converter.py``.

These are pure unit tests (no Postgres / Worker / Client). They verify that our
``dbosify.converter`` is internally correct and round-trips values,
exceptions, and type hints. Assertions are adapted to our actual behavior where
our lightweight (non-protobuf) ``Payload``/``Failure`` representation differs
from temporalio's; such adaptations are flagged with ``# Adapted:`` comments.
"""

from __future__ import annotations

import dataclasses
import inspect
import ipaddress
import logging
import sys
import traceback
from collections import deque
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, IntEnum
from typing import (
    Any,
    Dict,
    Literal,
    NewType,
    Optional,
    cast,
    get_args,
    get_type_hints,
)
from uuid import UUID, uuid4

import pytest
import typing_extensions
from typing_extensions import TypedDict

from dbosify.common import RawValue
from dbosify.converter import (
    AdvancedJSONEncoder,
    CompositePayloadConverter,
    DataConverter,
    DefaultPayloadConverter,
    JSONPlainPayloadConverter,
    JSONTypeConverter,
    JSONTypeConverterUnhandled,
    Payload,
    value_to_type,
)
from dbosify.exceptions import ApplicationError, FailureError

# StrEnum is available in 3.11+
if sys.version_info >= (3, 11):
    from enum import StrEnum


class NonSerializableClass:
    pass


class NonSerializableEnum(Enum):
    FOO = "foo"


class SerializableEnum(IntEnum):
    FOO = 1


if sys.version_info >= (3, 11):

    class SerializableStrEnum(StrEnum):
        FOO = "foo"


@dataclass
class MyDataClass:
    foo: str
    bar: int
    baz: SerializableEnum


@dataclass
class DatetimeClass:
    datetime: datetime


MyNewTypeStr = NewType("MyNewTypeStr", str)


@dataclass
class NewTypeMessage:
    data: dict[MyNewTypeStr, str]


async def test_converter_default() -> None:
    async def assert_payload(
        input: Any,
        expected_encoding: Any,
        expected_data: Any,
        *,
        expected_decoded_input: Any = None,
        type_hint: Any = None,
    ) -> Payload:
        payloads = await DataConverter().encode([input])
        # Check encoding and data
        assert len(payloads) == 1
        if isinstance(expected_encoding, str):
            expected_encoding = expected_encoding.encode()
        assert payloads[0].metadata["encoding"] == expected_encoding
        if isinstance(expected_data, str):
            expected_data = expected_data.encode()
        assert payloads[0].data == expected_data
        # Decode and check
        actual_inputs = await DataConverter().decode(payloads, [type_hint])
        assert len(actual_inputs) == 1
        if expected_decoded_input is None:
            expected_decoded_input = input
        assert type(actual_inputs[0]) is type(expected_decoded_input)
        assert actual_inputs[0] == expected_decoded_input
        return payloads[0]

    # Basic types
    await assert_payload(None, "binary/null", "")
    await assert_payload(b"some binary", "binary/plain", "some binary")
    # Adapted: dropped the json/protobuf WorkflowExecution case — our converter
    # has no protobuf payload converter (DESIGN: lightweight Payload, no proto).
    await assert_payload(
        {"foo": "bar", "baz": "qux"}, "json/plain", '{"baz":"qux","foo":"bar"}'
    )
    await assert_payload("somestr", "json/plain", '"somestr"')
    await assert_payload(1234, "json/plain", "1234")
    await assert_payload(12.34, "json/plain", "12.34")
    await assert_payload(True, "json/plain", "true")
    await assert_payload(False, "json/plain", "false")

    # Unknown type
    with pytest.raises(TypeError) as excinfo:
        await assert_payload(NonSerializableClass(), None, None)
    assert "not JSON serializable" in str(excinfo.value)

    # Bad enum type. We do not allow non-int or non-str enums due to ambiguity
    # in rebuilding and other confusion.
    with pytest.raises(TypeError) as excinfo:
        await assert_payload(NonSerializableEnum.FOO, None, None)
    assert "not JSON serializable" in str(excinfo.value)

    # Good enum no type hint
    await assert_payload(
        SerializableEnum.FOO, "json/plain", "1", expected_decoded_input=1
    )

    # Good enum type hint
    await assert_payload(
        SerializableEnum.FOO, "json/plain", "1", type_hint=SerializableEnum
    )

    # Data class without type hint is just dict
    await assert_payload(
        MyDataClass(foo="somestr", bar=123, baz=SerializableEnum.FOO),
        "json/plain",
        '{"bar":123,"baz":1,"foo":"somestr"}',
        expected_decoded_input={"foo": "somestr", "bar": 123, "baz": 1},
    )

    # Data class with type hint reconstructs the class
    await assert_payload(
        MyDataClass(foo="somestr", bar=123, baz=SerializableEnum.FOO),
        "json/plain",
        '{"bar":123,"baz":1,"foo":"somestr"}',
        type_hint=MyDataClass,
    )

    # Raw value
    await assert_payload(
        RawValue(Payload(metadata={"encoding": b"my-encoding"}, data=b"blah blah")),
        "my-encoding",
        "blah blah",
        type_hint=RawValue,
    )

    # Without type hint, it is deserialized as a str
    await assert_payload(
        datetime(2020, 1, 1, 1, 1, 1),
        "json/plain",
        '"2020-01-01T01:01:01"',
        expected_decoded_input="2020-01-01T01:01:01",
    )

    # With type hint, it is deserialized as a datetime
    await assert_payload(
        datetime(2020, 1, 1, 1, 1, 1, 1),
        "json/plain",
        '"2020-01-01T01:01:01.000001"',
        type_hint=datetime,
    )

    # Timezones work
    await assert_payload(
        datetime(2020, 1, 1, 1, 1, 1, tzinfo=timezone(timedelta(hours=5))),
        "json/plain",
        '"2020-01-01T01:01:01+05:00"',
        type_hint=datetime,
    )

    # Data class with datetime
    await assert_payload(
        DatetimeClass(datetime=datetime(2020, 1, 1, 1, 1, 1)),
        "json/plain",
        '{"datetime":"2020-01-01T01:01:01"}',
        type_hint=DatetimeClass,
    )

    # Newtype String
    await assert_payload(
        MyNewTypeStr("somestr"),
        "json/plain",
        '"somestr"',
        type_hint=MyNewTypeStr,
    )

    # Newtype String key
    await assert_payload(
        NewTypeMessage({MyNewTypeStr("key"): "value"}),
        "json/plain",
        '{"data":{"key":"value"}}',
        type_hint=NewTypeMessage,
    )


@pytest.mark.skip(
    reason="No protobuf payload converter: our Payload is a lightweight "
    "dataclass, not a protobuf Message; BinaryProtoPayloadConverter is absent."
)
def test_binary_proto() -> None: ...


@pytest.mark.skip(
    reason="encode_search_attribute_values is a protobuf-based API with no "
    "public analog in our converter (search-attribute encoding lives in "
    "_internal.attributes and operates on plain mappings, not proto values)."
)
def test_encode_search_attribute_values() -> None: ...


@pytest.mark.skip(
    reason="decode_search_attributes here decodes from a protobuf "
    "SearchAttributes message; our decode_search_attributes (in "
    "_internal.attributes) takes a plain Mapping, so this proto-shaped test "
    "does not map cleanly."
)
def test_decode_search_attributes() -> None: ...


NewIntType = NewType("NewIntType", int)
MyDataClassAlias = MyDataClass


@dataclass
class NestedDataClass:
    foo: str
    bar: list[NestedDataClass] = dataclasses.field(default_factory=list)
    baz: NestedDataClass | None = None
    qux: UUID | None = None


class MyTypedDict(TypedDict):
    foo: str
    bar: MyDataClass


class MyTypedDictNotTotal(TypedDict, total=False):
    foo: str
    bar: MyDataClass


# Sentinel for "no explicit expected result" in the json-type-hints helper.
# Adapted: a local sentinel stands in for temporalio.common._arg_unset.
_arg_unset = object()


def test_json_type_hints() -> None:
    converter = JSONPlainPayloadConverter()

    def ok(hint: Any, value: Any, expected_result: Any = _arg_unset) -> None:
        payload = converter.to_payload(value)
        assert payload
        converted_value = converter.from_payload(payload, hint)
        if expected_result is not _arg_unset:
            assert expected_result == converted_value
        else:
            assert converted_value == value

    def fail(hint: Any, value: Any) -> None:
        with pytest.raises(Exception):
            payload = converter.to_payload(value)
            assert payload
            converter.from_payload(payload, hint)

    # Primitives
    ok(int, 5)
    ok(int, 5.5, 5)
    ok(float, 5, 5.0)
    ok(float, 5.5)
    ok(bool, True)
    ok(str, "foo")
    ok(str, "foo")
    ok(bytes, b"foo")
    fail(int, "1")
    fail(float, "1")
    fail(bool, "1")
    fail(str, 1)

    # Any
    ok(Any, 5)
    ok(Any, None)

    # Literal
    ok(Literal["foo"], "foo")
    ok(Literal["foo", False], False)
    fail(Literal["foo", "bar"], "baz")
    ok(typing_extensions.Literal["foo"], "foo")
    ok(typing_extensions.Literal["foo", False], False)
    fail(typing_extensions.Literal["foo", "bar"], "baz")

    # Dataclass
    ok(MyDataClass, MyDataClass("foo", 5, SerializableEnum.FOO))
    ok(NestedDataClass, NestedDataClass("foo"))
    ok(NestedDataClass, NestedDataClass("foo", baz=NestedDataClass("bar")))
    ok(NestedDataClass, NestedDataClass("foo", bar=[NestedDataClass("bar")]))
    ok(NestedDataClass, NestedDataClass("foo", qux=uuid4()))
    # Missing required dataclass fields causes failure
    ok(NestedDataClass, {"foo": "bar"}, NestedDataClass("bar"))
    fail(NestedDataClass, {})
    # Additional dataclass fields is ok
    ok(NestedDataClass, {"foo": "bar", "unknownfield": "baz"}, NestedDataClass("bar"))

    # Optional/Union
    ok(int | None, 5)
    ok(int | None, None)
    ok(MyDataClass | None, MyDataClass("foo", 5, SerializableEnum.FOO))
    ok(int | str, 5)
    ok(int | str, "foo")
    ok(MyDataClass | NestedDataClass, MyDataClass("foo", 5, SerializableEnum.FOO))
    ok(MyDataClass | NestedDataClass, NestedDataClass("foo"))
    ok(int | None, None)
    ok(int | None, 5)
    fail(int | None, "1")
    ok(MyDataClass | NestedDataClass, MyDataClass("foo", 5, SerializableEnum.FOO))
    ok(MyDataClass | NestedDataClass, NestedDataClass("foo"))

    # NewType
    ok(NewIntType, 5)

    # List-like
    ok(list, [5])
    ok(list[int], [5])
    ok(list[MyDataClass], [MyDataClass("foo", 5, SerializableEnum.FOO)])
    ok(Iterable[int], [5, 6])
    ok(tuple[int, str], (5, "6"))
    ok(tuple[int, ...], (5, 6, 7))
    ok(set[int], {5, 6})
    ok(set, {5, 6})
    ok(list, ["foo"])
    ok(deque[int], deque([5, 6]))
    ok(Sequence[int], [5, 6])
    fail(list[int], [1, 2, "3"])

    # Dict-like
    ok(dict[str, MyDataClass], {"foo": MyDataClass("foo", 5, SerializableEnum.FOO)})
    ok(dict, {"foo": 123})
    ok(dict[str, Any], {"foo": 123})
    ok(dict[Any, int], {"foo": 123})
    ok(Mapping, {"foo": 123})
    ok(Mapping[str, int], {"foo": 123})
    ok(MutableMapping[str, int], {"foo": 123})
    ok(
        MyTypedDict,
        MyTypedDict(foo="somestr", bar=MyDataClass("foo", 5, SerializableEnum.FOO)),
    )
    # TypedDict allows all sorts of dicts, even missing-required/unknown fields,
    # matching Python runtime behavior of just accepting any dict.
    ok(MyTypedDictNotTotal, {"foo": "bar"})
    ok(MyTypedDict, {"foo": "bar", "blah": "meh"})

    # Non-string dict keys are supported
    ok(dict[int, str], {1: "1"})
    ok(dict[float, str], {1.0: "1"})
    ok(dict[bool, str], {True: "1"})

    # On a 3.10+ dict type, None isn't returned from a key. This is potentially a bug
    ok(dict[None, str], {"null": "1"})

    # Dict has a different value for None keys
    ok(Dict[None, str], {None: "1"})

    # Alias
    ok(MyDataClassAlias, MyDataClass("foo", 5, SerializableEnum.FOO))

    # IntEnum
    ok(SerializableEnum, SerializableEnum.FOO)
    ok(list[SerializableEnum], [SerializableEnum.FOO, SerializableEnum.FOO])

    # UUID
    ok(UUID, uuid4())
    ok(list[UUID], [uuid4(), uuid4()])

    # StrEnum is available in 3.11+
    if sys.version_info >= (3, 11):
        # StrEnum
        ok(SerializableStrEnum, SerializableStrEnum.FOO)
        ok(
            list[SerializableStrEnum],
            [SerializableStrEnum.FOO, SerializableStrEnum.FOO],
        )

    # Adapted: dropped the pydantic cases — pydantic data conversion is not
    # supported (per task rules).


# This is an example of appending the stack to every Temporal failure error
def append_temporal_stack(exc: BaseException | None) -> None:
    while exc:
        # Only append if it doesn't appear already there
        if (
            isinstance(exc, FailureError)
            and exc.failure
            and exc.failure.stack_trace
            and len(exc.args) == 1
            and "\nStack:\n" not in exc.args[0]
        ):
            exc.args = (f"{exc}\nStack:\n{exc.failure.stack_trace.rstrip()}",)
        exc = exc.__cause__


async def test_exception_format() -> None:
    # Cause a nested exception
    actual_err: Exception
    try:
        try:
            raise ValueError("error1")
        except Exception as err:
            raise RuntimeError("error2") from err
    except Exception as err:
        actual_err = err
    assert actual_err

    # Convert to failure and back. Adapted: encode_failure(exc) returns the
    # Failure envelope (a dict) that decode_failure takes.
    failure = await DataConverter.default.encode_failure(actual_err)
    failure_error = await DataConverter.default.decode_failure(failure)
    # Confirm type is prepended
    assert isinstance(failure_error, ApplicationError)
    assert "RuntimeError: error2" == str(failure_error)
    assert isinstance(failure_error.cause, ApplicationError)
    assert "ValueError: error1" == str(failure_error.cause)

    # Append the stack and format the exception and check the output
    append_temporal_stack(failure_error)
    output = "".join(
        traceback.format_exception(
            type(failure_error), failure_error, failure_error.__traceback__
        )
    )
    # Adapted: our exception type lives in dbosify.exceptions, not
    # temporalio.exceptions.
    assert "dbosify.exceptions.ApplicationError: ValueError: error1" in output
    assert "dbosify.exceptions.ApplicationError: RuntimeError: error" in output
    assert output.count("\nStack:\n") == 2

    # This shows how it might look for those with debugging on
    logging.getLogger(__name__).debug(
        "Showing appended exception", exc_info=failure_error
    )


@pytest.mark.skip(
    reason="Protobuf-specific: this test constructs/mutates a protobuf Failure "
    "(application_failure_info.details.payloads, encoded_attributes.metadata) "
    "and a Payloads-wrapping codec. Our Failure is a plain dict envelope and "
    "the encoded-attributes flag is inert, so this does not map."
)
async def test_failure_encoded_attributes() -> None: ...


class IPv4AddressPayloadConverter(CompositePayloadConverter):
    def __init__(self) -> None:
        # Replace default JSON plain with our own that has our type converter
        json_converter = JSONPlainPayloadConverter(
            encoder=IPv4AddressJSONEncoder,
            custom_type_converters=[IPv4AddressJSONTypeConverter()],
        )
        super().__init__(
            *[
                c if not isinstance(c, JSONPlainPayloadConverter) else json_converter
                for c in DefaultPayloadConverter.default_encoding_payload_converters
            ]
        )


class IPv4AddressJSONEncoder(AdvancedJSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, ipaddress.IPv4Address):
            return str(o)
        return super().default(o)


class IPv4AddressJSONTypeConverter(JSONTypeConverter):
    def to_typed_value(
        self, hint: type, value: Any
    ) -> Any | None | JSONTypeConverterUnhandled:
        if inspect.isclass(hint) and issubclass(hint, ipaddress.IPv4Address):
            return ipaddress.IPv4Address(value)
        return JSONTypeConverter.Unhandled


def test_json_type_converter_unhandled_type_public() -> None:
    return_type = get_type_hints(JSONTypeConverter.to_typed_value)["return"]

    assert JSONTypeConverterUnhandled.__name__ == "JSONTypeConverterUnhandled"
    assert JSONTypeConverterUnhandled in get_args(return_type)
    assert JSONTypeConverterUnhandled(JSONTypeConverter.Unhandled) is (
        JSONTypeConverter.Unhandled
    )


async def test_json_type_converter() -> None:
    addr = ipaddress.IPv4Address("1.2.3.4")
    custom_conv = dataclasses.replace(
        DataConverter.default, payload_converter_class=IPv4AddressPayloadConverter
    )

    # Fails to encode with default
    with pytest.raises(TypeError):
        await DataConverter.default.encode([addr])
    with pytest.raises(TypeError):
        await DataConverter.default.encode([[addr, addr]])

    # But encodes with custom
    payload = (await custom_conv.encode([addr]))[0]
    assert '"1.2.3.4"' == payload.data.decode()
    list_payload = (await custom_conv.encode([[addr, addr]]))[0]
    assert '["1.2.3.4","1.2.3.4"]' == list_payload.data.decode()

    # Fails to decode with default
    with pytest.raises(TypeError):
        await DataConverter.default.decode([payload], [ipaddress.IPv4Address])
    with pytest.raises(TypeError):
        await DataConverter.default.decode(
            [list_payload], [list[ipaddress.IPv4Address]]
        )

    # But decodes with custom
    assert addr == (await custom_conv.decode([payload], [ipaddress.IPv4Address]))[0]
    assert [addr, addr] == (
        await custom_conv.decode([list_payload], [list[ipaddress.IPv4Address]])
    )[0]


def test_value_to_type_literal_key() -> None:
    # The type for the dictionary's *key*:
    KeyHint = Literal[
        "Key1",
        "Key2",
    ]

    # The type for the dictionary's *value* (the inner dict):
    InnerKeyHint = Literal[
        "Inner1",
        "Inner2",
    ]
    InnerValueHint = Optional[str | int | float]
    ValueHint = dict[InnerKeyHint, InnerValueHint]

    # The full type hint for the mapping:
    hint_with_bug = dict[KeyHint, ValueHint]

    # A value that uses one of the literal keys:
    value_to_convert: dict[str, Any] = {"Key1": {"Inner1": 123.45, "Inner2": 10}}
    custom_converters: Sequence[JSONTypeConverter] = []

    # Function executes without error
    value_to_type(cast(type, hint_with_bug), value_to_convert, custom_converters)
