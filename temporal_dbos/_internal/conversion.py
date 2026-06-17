"""The data-conversion boundary (DESIGN §6.9).

User values are converted to/from small tagged payload dicts at the Temporal
boundaries — where function signatures supply the type hints that rebuild the
original Python types, and where the (async)
:py:class:`~temporal_dbos.converter.PayloadCodec` can run. The DBOS serializer
sees only those JSON-safe dicts, never raw user values.

This module owns the process's active ``DataConverter`` (set by ``Worker`` /
``Client`` from their ``data_converter=`` argument) and the encode/decode
helpers the dispatcher, interpreter, and client call. Every start path encodes
its args, so the decode side always receives payload dicts.
"""

import base64
import inspect
import json
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    get_type_hints,
)

from ..converter import DataConverter, Payload

# What gets embedded in checkpoints for a user value: a small dict (DESIGN
# §6.9 envelope), NOT a raw Payload. This keeps DBOS observability (Conductor,
# list_workflows) readable — a json/plain payload with no codec is stored as
# its *inline* JSON value (the actual fields show up in the dashboard), and
# only genuinely-binary or codec-transformed bytes fall back to base64.
_active_converter: DataConverter = DataConverter.default


def _payload_to_dict(payload: Payload, *, inline_json: bool) -> Dict[str, Any]:
    encoding = payload.metadata.get("encoding", b"").decode()
    out: Dict[str, Any] = {"encoding": encoding}
    # Preserve any metadata a custom converter/codec attached beyond the
    # encoding (e.g. an encryption key id) — base64 since values are bytes.
    extra = {
        k: base64.b64encode(v).decode()
        for k, v in payload.metadata.items()
        if k != "encoding"
    }
    if extra:
        out["meta"] = extra
    if inline_json and encoding == "json/plain":
        # The actual user value, readable in DBOS tooling.
        out["json"] = json.loads(payload.data)
    else:
        # Binary, or codec-transformed (opaque by design) bytes.
        out["b64"] = base64.b64encode(payload.data).decode()
    return out


def _payload_from_dict(d: Dict[str, Any]) -> Payload:
    metadata: Dict[str, bytes] = {"encoding": str(d["encoding"]).encode()}
    for key, value in d.get("meta", {}).items():
        metadata[key] = base64.b64decode(value)
    if "json" in d:
        data = json.dumps(d["json"], separators=(",", ":"), sort_keys=True).encode()
    else:
        data = base64.b64decode(d["b64"])
    return Payload(metadata=metadata, data=data)


def set_converter(converter: Optional[DataConverter]) -> None:
    """Install the process's active data converter (``None`` resets to the
    default). Called by ``Worker`` and ``Client`` construction."""
    global _active_converter
    _active_converter = converter if converter is not None else DataConverter.default


def get_converter() -> DataConverter:
    """The process's active data converter."""
    return _active_converter


def reset_converter() -> None:
    """Restore the default converter (test teardown)."""
    set_converter(None)


async def encode_values(
    values: Sequence[Any], converter: Optional[DataConverter] = None
) -> List[Dict[str, Any]]:
    """Convert user values to embeddable payload dicts (codec-encoding the
    bytes when a codec is configured). ``converter`` overrides the process
    converter for this call (e.g. an ``AsyncActivityHandle`` per-handle
    data converter)."""
    conv = converter if converter is not None else _active_converter
    inline = conv.payload_codec is None
    payloads = await conv.encode(list(values))
    return [_payload_to_dict(p, inline_json=inline) for p in payloads]


async def decode_values(
    values: Sequence[Any], type_hints: Optional[Sequence[type]] = None
) -> List[Any]:
    """Convert payload dicts back to user values, using ``type_hints`` to
    rebuild the original types."""
    items = list(values)
    if not items:
        return items
    payloads = [_payload_from_dict(v) for v in items]
    # Apply hints per position (like temporalio's zip_longest): slicing covers
    # both extra hints (default-valued params, called with fewer args) and the
    # short/absent case (extra payloads decode hint-free). A whole-list length
    # check would instead drop ALL hints whenever the arity differs.
    hints = list(type_hints)[: len(payloads)] if type_hints is not None else None
    return await _active_converter.decode(payloads, hints)


async def encode_value(
    value: Any, converter: Optional[DataConverter] = None
) -> Dict[str, Any]:
    """Encode a single user value to an embeddable payload dict. ``converter``
    overrides the process converter for this call."""
    return (await encode_values([value], converter))[0]


async def decode_value(value: Any, type_hint: Optional[type] = None) -> Any:
    """Decode a single payload dict back to a user value."""
    hints = [type_hint] if type_hint is not None else None
    return (await decode_values([value], hints))[0]


def encode_values_sync(
    values: Sequence[Any], converter: Optional[DataConverter] = None
) -> List[Dict[str, Any]]:
    """Payload-only (no codec) encode for the rare sync caller — the Phase-0
    dispatcher helpers and failure-detail encoding. Codecs are async, so they
    do not apply here. ``converter`` overrides the process converter (e.g. a
    per-handle async-activity converter encoding failure details)."""
    conv = converter if converter is not None else _active_converter
    payloads = conv.payload_converter.to_payloads(list(values))
    return [_payload_to_dict(p, inline_json=True) for p in payloads]


def encode_value_sync(value: Any) -> Dict[str, Any]:
    """Payload-only (no codec) single-value encode (e.g. cron last-completion,
    read back synchronously by ``workflow.get_last_completion_result``)."""
    return encode_values_sync([value])[0]


def decode_value_sync(value: Any, type_hint: Optional[type] = None) -> Any:
    """Payload-only (no codec) decode for the Phase-0 dispatcher helpers."""
    payload = _payload_from_dict(value)
    hints = [type_hint] if type_hint is not None else None
    return _active_converter.payload_converter.from_payloads([payload], hints)[0]


# ---------------------------------------------------------------------------
# Headers (interceptor header-propagation channel, DEVIATIONS D24).
#
# At the interceptor boundary a header value is a :py:class:`Payload` (as in
# temporalio — the user encodes/decodes it with ``workflow.payload_converter``/
# ``activity.payload_converter``). On the wire (run meta, inbox envelopes,
# activity step meta) it is the same small payload dict every other value uses,
# so it checkpoints as JSON. A configured ``PayloadCodec`` runs on header bytes
# too (like args), so an encrypting codec protects header values; both are async
# and called only on the real loop, at the same boundaries args are encoded.
# ---------------------------------------------------------------------------


async def encode_headers(headers: Optional[Mapping[str, Payload]]) -> Dict[str, Any]:
    """Convert boundary headers (str -> Payload) to wire form (str -> payload
    dict), codec-encoding the bytes when a codec is configured. Empty/None maps
    to ``{}``."""
    if not headers:
        return {}
    keys = list(headers.keys())
    payloads: Sequence[Payload] = list(headers.values())
    inline = _active_converter.payload_codec is None
    if _active_converter.payload_codec is not None:
        payloads = await _active_converter.payload_codec.encode(list(payloads))
    return {
        key: _payload_to_dict(payload, inline_json=inline)
        for key, payload in zip(keys, payloads)
    }


async def decode_headers(wire: Optional[Mapping[str, Any]]) -> Dict[str, Payload]:
    """Convert wire-form headers (str -> payload dict) back to boundary headers
    (str -> Payload), codec-decoding the bytes when a codec is configured.
    Empty/None maps to ``{}``."""
    if not wire:
        return {}
    keys = list(wire.keys())
    payloads: Sequence[Payload] = [_payload_from_dict(value) for value in wire.values()]
    if _active_converter.payload_codec is not None:
        payloads = await _active_converter.payload_codec.decode(list(payloads))
    return dict(zip(keys, payloads))


def type_hints_from_func(
    func: Callable[..., Any],
) -> Tuple[Optional[List[type]], Optional[type]]:
    """Extract (positional arg types, return type) from a function's
    annotations, mirroring ``temporalio.common._type_hints_from_func``.

    Arg types are ``None`` if any parameter is non-positional or unannotated
    (so we either type all args or none). A leading unannotated ``self`` is
    skipped.
    """
    try:
        sig = inspect.signature(func)
        hints = get_type_hints(func)
    except (TypeError, ValueError, NameError):
        return None, None
    ret_hint = hints.get("return")
    ret: Optional[type] = ret_hint if ret_hint is not inspect.Signature.empty else None
    args: List[type] = []
    for index, value in enumerate(sig.parameters.values()):
        if (
            index == 0
            and value.name == "self"
            and value.annotation is inspect.Parameter.empty
        ):
            continue
        if value.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return None, ret
        arg_hint = hints.get(value.name)
        if arg_hint is None:
            return None, ret
        args.append(arg_hint)
    return args, ret
