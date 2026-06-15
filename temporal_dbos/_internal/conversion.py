"""The data-conversion boundary (DESIGN §6.9).

User values are converted to/from :py:class:`~temporal_dbos.converter.Payload`
records at the Temporal boundaries — where function signatures supply the type
hints that rebuild the original Python types, and where the (async)
:py:class:`~temporal_dbos.converter.PayloadCodec` can run. The DBOS serializer
sees only the resulting JSON-safe ``Payload`` records, never raw user values.

This module owns the process's active ``DataConverter`` (set by ``Worker`` /
``Client`` from their ``data_converter=`` argument) and the encode/decode
helpers the dispatcher, interpreter, and client call.

During the staged migration, decoding is **tolerant**: only ``Payload``
elements are converted, so internal/legacy paths that still pass raw values
(e.g. the Phase-0 dispatcher helpers) keep working. A raw value is already the
right Python type, so passing it through is correct.
"""

import inspect
from typing import Any, Callable, List, Optional, Sequence, Tuple, get_type_hints

from ..converter import DataConverter, Payload

_active_converter: DataConverter = DataConverter.default


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


async def encode_values(values: Sequence[Any]) -> List[Payload]:
    """Convert user values to payloads (and codec-encode the bytes)."""
    return await _active_converter.encode(list(values))


async def decode_values(
    values: Sequence[Any], type_hints: Optional[Sequence[type]] = None
) -> List[Any]:
    """Convert payloads back to user values, using ``type_hints`` to rebuild
    the original types. Tolerant: a sequence with no ``Payload`` elements is
    returned unchanged (a legacy/internal raw path)."""
    items = list(values)
    if not items or not all(isinstance(v, Payload) for v in items):
        return items
    hints: Optional[List[type]] = None
    if type_hints is not None and len(type_hints) == len(items):
        hints = list(type_hints)
    return await _active_converter.decode(items, hints)


def encode_values_sync(values: Sequence[Any]) -> List[Payload]:
    """Payload-only (no codec) encode for the rare sync caller — the Phase-0
    dispatcher helpers. Codecs are async, so they do not apply here; these
    helpers are internal/test-only (superseded by the ``Client`` facade)."""
    return _active_converter.payload_converter.to_payloads(list(values))


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
