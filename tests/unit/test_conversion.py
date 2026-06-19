"""Unit tests for the embeddable payload-dict representation
(dbosify._internal.conversion). No database needed.

These pin the on-disk shape that DBOS observability tools (Conductor,
list_workflows) render: json/plain values inline (readable), everything else
base64, and any extra Payload metadata preserved across the round-trip.
"""

import base64
import json
from dataclasses import dataclass

from dbosify._internal.conversion import (
    _payload_from_dict,
    _payload_to_dict,
    decode_values,
    encode_values,
)
from dbosify.converter import Payload


@dataclass
class _Point:
    x: int
    y: int


def test_json_plain_is_stored_inline() -> None:
    p = Payload(
        metadata={"encoding": b"json/plain"}, data=b'{"greeting":"Hi","name":"Bo"}'
    )
    d = _payload_to_dict(p, inline_json=True)
    # The actual fields are visible (this is what Conductor shows).
    assert d == {
        "encoding": "json/plain",
        "json": {"greeting": "Hi", "name": "Bo"},
    }
    back = _payload_from_dict(d)
    assert json.loads(back.data) == json.loads(p.data)
    assert back.metadata == {"encoding": b"json/plain"}


def test_bytes_uses_base64() -> None:
    p = Payload(metadata={"encoding": b"binary/plain"}, data=b"\x00\x01\x02raw")
    d = _payload_to_dict(p, inline_json=True)
    assert d["encoding"] == "binary/plain" and "b64" in d and "json" not in d
    assert _payload_from_dict(d) == p


def test_none_roundtrips() -> None:
    p = Payload(metadata={"encoding": b"binary/null"})
    assert _payload_from_dict(_payload_to_dict(p, inline_json=True)) == p


def test_codec_disables_inline() -> None:
    # When a codec is configured the bytes are opaque, so even json/plain is
    # stored base64 (callers pass inline_json=False).
    p = Payload(metadata={"encoding": b"json/plain"}, data=b'{"a":1}')
    d = _payload_to_dict(p, inline_json=False)
    assert "b64" in d and "json" not in d


def test_extra_metadata_is_preserved() -> None:
    # A custom codec may attach metadata (e.g. an encryption key id); it must
    # survive the round-trip so the codec can decode.
    p = Payload(
        metadata={
            "encoding": b"binary/encrypted",
            "key-id": b"k7",
            "nonce": b"\xff\x00",
        },
        data=b"ciphertext",
    )
    d = _payload_to_dict(p, inline_json=False)
    assert d["meta"] == {
        "key-id": base64.b64encode(b"k7").decode(),
        "nonce": base64.b64encode(b"\xff\x00").decode(),
    }
    assert _payload_from_dict(d) == p


async def test_more_hints_than_values_keeps_per_position_hint() -> None:
    # Fewer args than typed params (rest default-valued): the value present must
    # still be reconstructed from its hint, not dropped to a plain dict.
    encoded = await encode_values([_Point(1, 2)])
    decoded = await decode_values(encoded, [_Point, _Point])
    assert decoded == [_Point(1, 2)] and isinstance(decoded[0], _Point)


async def test_more_values_than_hints_decodes_extra_hint_free() -> None:
    # The inverse: extra payloads beyond the supplied hints decode without a
    # hint (a plain dict for a JSON object) rather than erroring.
    encoded = await encode_values([_Point(1, 2), _Point(3, 4)])
    decoded = await decode_values(encoded, [_Point])
    assert isinstance(decoded[0], _Point)
    assert decoded[1] == {"x": 3, "y": 4} and isinstance(decoded[1], dict)
