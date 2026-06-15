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
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, get_type_hints

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


async def encode_values(values: Sequence[Any]) -> List[Dict[str, Any]]:
    """Convert user values to embeddable payload dicts (codec-encoding the
    bytes when a codec is configured)."""
    inline = _active_converter.payload_codec is None
    payloads = await _active_converter.encode(list(values))
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
    hints: Optional[List[type]] = None
    if type_hints is not None and len(type_hints) == len(payloads):
        hints = list(type_hints)
    return await _active_converter.decode(payloads, hints)


def encode_values_sync(values: Sequence[Any]) -> List[Dict[str, Any]]:
    """Payload-only (no codec) encode for the rare sync caller — the Phase-0
    dispatcher helpers. Codecs are async, so they do not apply here; these
    helpers are internal/test-only (superseded by the ``Client`` facade)."""
    payloads = _active_converter.payload_converter.to_payloads(list(values))
    return [_payload_to_dict(p, inline_json=True) for p in payloads]


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
