"""Unit tests for the embeddable payload-dict representation
(temporal_dbos._internal.conversion). No database needed.

These pin the on-disk shape that DBOS observability tools (Conductor,
list_workflows) render: json/plain values inline (readable), everything else
base64, and any extra Payload metadata preserved across the round-trip.
"""

import base64
import json

from temporal_dbos._internal.conversion import _payload_from_dict, _payload_to_dict
from temporal_dbos.converter import Payload


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
