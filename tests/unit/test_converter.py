"""Unit tests for the data-conversion pipeline (temporal_dbos.converter).

No database needed. The crux these cover: JSON forgets Python types, so
without a hint a value comes back as a plain dict/scalar (temporalio's
default behavior), and *with* a hint it is rebuilt into the original type.
"""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pytest

from temporal_dbos.common import RawValue
from temporal_dbos.converter import (
    BinaryNullPayloadConverter,
    DataConverter,
    DefaultPayloadConverter,
    JSONPlainPayloadConverter,
    Payload,
    PayloadCodec,
    PayloadConverter,
    value_to_type,
)

PC = PayloadConverter.default


def roundtrip(value: Any, hint: Any = None) -> Any:
    """Encode then decode a single value through the default converter."""
    payload = PC.to_payload(value)
    return PC.from_payload(payload, hint)


def encoding_of(value: Any) -> str:
    return PC.to_payload(value).metadata["encoding"].decode()


# --------------------------------------------------------------------------
# Encoding selection.
# --------------------------------------------------------------------------
def test_encoding_selection() -> None:
    assert encoding_of(None) == "binary/null"
    assert encoding_of(b"bytes") == "binary/plain"
    assert encoding_of("hi") == "json/plain"
    assert encoding_of(42) == "json/plain"
    assert encoding_of({"a": 1}) == "json/plain"


def test_default_converter_chain_has_no_protobuf() -> None:
    encodings = [
        c.encoding for c in DefaultPayloadConverter.default_encoding_payload_converters
    ]
    assert encodings == ["binary/null", "binary/plain", "json/plain"]


# --------------------------------------------------------------------------
# Primitives round-trip (with and without hints).
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "value,hint",
    [
        (None, type(None)),
        (True, bool),
        (42, int),
        (3.5, float),
        ("hello", str),
        (b"raw bytes", bytes),
        ([1, 2, 3], List[int]),
        ({"a": 1, "b": 2}, Dict[str, int]),
    ],
)
def test_primitive_roundtrip(value: Any, hint: Any) -> None:
    assert roundtrip(value, hint) == value
    # Without a hint, JSON-native values come back unchanged too.
    assert roundtrip(value) == value


def test_bytes_roundtrip_preserves_type() -> None:
    out = roundtrip(b"\x00\x01hi", bytes)
    assert out == b"\x00\x01hi"
    assert isinstance(out, bytes)


# --------------------------------------------------------------------------
# The crux: dataclass with vs. without a type hint.
# --------------------------------------------------------------------------
@dataclass
class Greeting:
    name: str
    times: int


def test_dataclass_without_hint_is_a_dict() -> None:
    out = roundtrip(Greeting(name="bob", times=3))
    assert out == {"name": "bob", "times": 3}
    assert isinstance(out, dict)


def test_dataclass_with_hint_is_reconstructed() -> None:
    out = roundtrip(Greeting(name="bob", times=3), Greeting)
    assert out == Greeting(name="bob", times=3)
    assert isinstance(out, Greeting)


@dataclass
class Outer:
    inner: Greeting
    tags: List[str]


def test_nested_dataclass_roundtrip() -> None:
    value = Outer(inner=Greeting("x", 1), tags=["a", "b"])
    out = roundtrip(value, Outer)
    assert out == value
    assert isinstance(out.inner, Greeting)


@dataclass
class HasBytes:
    blob: bytes


def test_bytes_nested_in_dataclass() -> None:
    value = HasBytes(blob=b"hi")
    out = roundtrip(value, HasBytes)
    assert out == value
    assert isinstance(out.blob, bytes)


# --------------------------------------------------------------------------
# datetime, UUID, enums.
# --------------------------------------------------------------------------
def test_datetime_roundtrip() -> None:
    naive = datetime(2026, 6, 15, 12, 0, 0)
    assert roundtrip(naive, datetime) == naive
    aware = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    assert roundtrip(aware, datetime) == aware
    # No hint -> ISO string.
    assert roundtrip(naive) == "2026-06-15T12:00:00"


def test_uuid_roundtrip() -> None:
    u = uuid.uuid4()
    out = roundtrip(u, uuid.UUID)
    assert out == u
    assert isinstance(out, uuid.UUID)
    assert roundtrip(u) == str(u)  # no hint -> str


class Color(IntEnum):
    RED = 1
    GREEN = 2


def test_int_enum_roundtrip() -> None:
    out = roundtrip(Color.RED, Color)
    assert out is Color.RED
    assert roundtrip(Color.GREEN) == 2  # no hint -> int


# --------------------------------------------------------------------------
# Optional / Union and collections.
# --------------------------------------------------------------------------
def test_optional_roundtrip() -> None:
    assert roundtrip(5, Optional[int]) == 5
    assert roundtrip(None, Optional[int]) is None


def test_tuple_roundtrip() -> None:
    out = roundtrip((1, "x"), Tuple[int, str])
    assert out == (1, "x")
    assert isinstance(out, tuple)


def test_set_roundtrip() -> None:
    out = roundtrip({1, 2, 3}, Set[int])
    assert out == {1, 2, 3}
    assert isinstance(out, set)


def test_list_of_dataclass_roundtrip() -> None:
    value = [Greeting("a", 1), Greeting("b", 2)]
    out = roundtrip(value, List[Greeting])
    assert out == value
    assert all(isinstance(g, Greeting) for g in out)


def test_dict_with_int_keys_recovers_key_type() -> None:
    # JSON stringifies keys; the hint recovers the int key type.
    out = roundtrip({1: "a", 2: "b"}, Dict[int, str])
    assert out == {1: "a", 2: "b"}
    assert all(isinstance(k, int) for k in out)


# --------------------------------------------------------------------------
# value_to_type directly.
# --------------------------------------------------------------------------
def test_value_to_type_rejects_bad_shape() -> None:
    with pytest.raises(TypeError):
        value_to_type(Greeting, ["not", "a", "dict"])


# --------------------------------------------------------------------------
# RawValue passthrough.
# --------------------------------------------------------------------------
def test_raw_value_passthrough() -> None:
    p = Payload(metadata={"encoding": b"json/plain"}, data=b'"hello"')
    assert PC.to_payloads([RawValue(p)]) == [p]
    out = PC.from_payloads([p], [RawValue])
    assert isinstance(out[0], RawValue)
    assert out[0].payload == p


# --------------------------------------------------------------------------
# Determinism and failure modes.
# --------------------------------------------------------------------------
def test_json_output_is_deterministic() -> None:
    a = PC.to_payload({"b": 2, "a": 1})
    b = PC.to_payload({"a": 1, "b": 2})
    assert a.data == b.data  # sort_keys


def test_unserializable_value_fails_loudly() -> None:
    with pytest.raises(TypeError):
        PC.to_payload(object())


def test_unknown_encoding_raises_on_decode() -> None:
    p = Payload(metadata={"encoding": b"json/protobuf"}, data=b"{}")
    with pytest.raises(KeyError):
        PC.from_payloads([p])


def test_binary_null_rejects_nonempty_data() -> None:
    conv = BinaryNullPayloadConverter()
    with pytest.raises(RuntimeError):
        conv.from_payload(Payload(metadata={"encoding": b"binary/null"}, data=b"x"))


# --------------------------------------------------------------------------
# Codec pipeline (DataConverter.encode/decode is async).
# --------------------------------------------------------------------------
class _ReverseCodec(PayloadCodec):
    """Toy codec: reverses the payload bytes (stands in for encrypt/compress)."""

    async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
        return [Payload(metadata=p.metadata, data=p.data[::-1]) for p in payloads]

    async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
        return [Payload(metadata=p.metadata, data=p.data[::-1]) for p in payloads]


def test_codec_roundtrip() -> None:
    dc = DataConverter(payload_codec=_ReverseCodec())
    payloads = asyncio.run(dc.encode([{"a": 1}]))
    # On disk the bytes are transformed (reversed), not the plain JSON.
    assert payloads[0].data != PC.to_payload({"a": 1}).data
    out = asyncio.run(dc.decode(payloads, [dict]))
    assert out == [{"a": 1}]


def test_default_dataconverter_encode_decode_matches_payload_converter() -> None:
    dc = DataConverter.default
    payloads = asyncio.run(dc.encode([Greeting("z", 9)]))
    assert payloads == PC.to_payloads([Greeting("z", 9)])
    assert asyncio.run(dc.decode(payloads, [Greeting])) == [Greeting("z", 9)]


def test_custom_json_encoding_name() -> None:
    conv = JSONPlainPayloadConverter(encoding="json/custom")
    assert conv.encoding == "json/custom"
    payload = conv.to_payload({"x": 1})
    assert payload is not None
    assert payload.metadata["encoding"] == b"json/custom"
