"""Data conversion, mirroring ``temporalio.converter`` (DESIGN §6.9).

This converts Python values to/from encoding-tagged :py:class:`Payload`
records and (optionally) runs a :py:class:`PayloadCodec` over the payload
bytes for encryption/compression. The public surface mirrors temporalio so
custom ``DataConverter`` / ``PayloadConverter`` / ``PayloadCodec`` subclasses
written against temporalio work unchanged.

Deliberate omissions (kept lean):

* **Protobuf payloads** — the ``json/protobuf`` and ``binary/protobuf``
  encoders are not provided (a corollary of D1: no non-Python clients, so a
  cross-language proto schema buys nothing here). A raw ``protobuf.Message``
  passed as a payload falls through to the JSON encoder and fails *loudly* at
  encode time rather than being silently mishandled.
* **External storage, payload-size limits, search-attribute helpers** — not
  part of this module.

Type-hint-driven reconstruction (:py:func:`value_to_type`) is the crux: JSON
forgets Python types, so decoding rebuilds dataclasses/enums/datetime/UUID/etc.
from the type hints supplied by workflow/activity/handler signatures. Without
a hint a value comes back as a plain dict/list/scalar — exactly temporalio's
default-converter behavior.
"""

from __future__ import annotations

import collections
import collections.abc
import dataclasses
import functools
import inspect
import json
import sys
import typing
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from itertools import zip_longest
from types import UnionType
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    NewType,
    Optional,
    Type,
    TypeVar,
    cast,
    get_type_hints,
)

from .common import RawValue

if sys.version_info >= (3, 11):
    from enum import StrEnum

__all__ = [
    "ActivitySerializationContext",
    "AdvancedJSONEncoder",
    "BinaryNullPayloadConverter",
    "BinaryPlainPayloadConverter",
    "CompositePayloadConverter",
    "DataConverter",
    "DefaultFailureConverter",
    "DefaultFailureConverterWithEncodedAttributes",
    "DefaultPayloadConverter",
    "EncodingPayloadConverter",
    "FailureConverter",
    "JSONPlainPayloadConverter",
    "JSONTypeConverter",
    "JSONTypeConverterUnhandled",
    "Payload",
    "PayloadCodec",
    "PayloadConverter",
    "SerializationContext",
    "WithSerializationContext",
    "WorkflowSerializationContext",
    "default",
    "value_to_type",
]


# ---------------------------------------------------------------------------
# Payload — our local stand-in for temporalio.api.common.v1.Payload (a
# protobuf). Same logical shape: encoding-tagged metadata + raw bytes.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Payload:
    """An encoding-tagged unit of serialized data.

    ``metadata["encoding"]`` (bytes) selects the converter on the way back;
    ``data`` is the raw serialized bytes a :py:class:`PayloadCodec` operates on.
    """

    metadata: Mapping[str, bytes] = field(default_factory=dict)
    data: bytes = b""


# ---------------------------------------------------------------------------
# Serialization context (mirrors temporalio.converter._serialization_context).
# ---------------------------------------------------------------------------
class SerializationContext(ABC):
    """Base serialization context, providing contextual information during
    (de)serialization. See :py:class:`WithSerializationContext`."""


@dataclass(frozen=True)
class WorkflowSerializationContext(SerializationContext):
    """Serialization context for a workflow's payloads."""

    namespace: str
    workflow_id: str


@dataclass(frozen=True)
class ActivitySerializationContext(SerializationContext):
    """Serialization context for an activity's payloads."""

    namespace: str
    activity_id: Optional[str]
    activity_type: Optional[str]
    activity_task_queue: Optional[str]
    workflow_id: Optional[str]
    workflow_type: Optional[str]
    is_local: bool


class WithSerializationContext(ABC):
    """Interface for converters/codecs that consume a
    :py:class:`SerializationContext`. During conversion, an implementer is
    replaced by ``with_context(context)`` so its methods can use the context.
    """

    def with_context(self, context: SerializationContext) -> "WithSerializationContext":
        """Return a copy configured to use ``context``."""
        raise NotImplementedError()


# ---------------------------------------------------------------------------
# Payload converters.
# ---------------------------------------------------------------------------
class PayloadConverter(ABC):
    """Base payload converter to/from multiple payloads/values."""

    default: ClassVar["PayloadConverter"]
    """Default payload converter."""

    @abstractmethod
    def to_payloads(self, values: Sequence[Any]) -> List[Payload]:
        """Encode values into payloads. Implementers pass
        :py:class:`temporal_dbos.common.RawValue` through unchanged."""
        raise NotImplementedError

    @abstractmethod
    def from_payloads(
        self, payloads: Sequence[Payload], type_hints: Optional[List[type]] = None
    ) -> List[Any]:
        """Decode payloads into values. ``type_hints``, if present, must be the
        same length as ``payloads``. A hint of ``RawValue`` yields the raw
        payload wrapped in :py:class:`RawValue`."""
        raise NotImplementedError

    def to_payload(self, value: Any) -> Payload:
        """Convert a single value to a payload."""
        return self.to_payloads([value])[0]

    def from_payload(self, payload: Payload, type_hint: Optional[type] = None) -> Any:
        """Convert a single payload to a value, optionally type-hinted."""
        return self.from_payloads([payload], [type_hint] if type_hint else None)[0]


class EncodingPayloadConverter(ABC):
    """Base converter to/from a single payload/value with a known encoding,
    for use inside :py:class:`CompositePayloadConverter`."""

    @property
    @abstractmethod
    def encoding(self) -> str:
        """Encoding this converter handles."""
        raise NotImplementedError

    @abstractmethod
    def to_payload(self, value: Any) -> Optional[Payload]:
        """Encode a value to a payload, or ``None`` if it cannot convert it
        (so the composite can try the next converter)."""
        raise NotImplementedError

    @abstractmethod
    def from_payload(self, payload: Payload, type_hint: Optional[type] = None) -> Any:
        """Decode a payload (whose encoding matches) to a value."""
        raise NotImplementedError


class CompositePayloadConverter(PayloadConverter, WithSerializationContext):
    """Delegates to an ordered list of :py:class:`EncodingPayloadConverter`,
    trying each in turn for encoding and selecting by encoding for decoding."""

    converters: Mapping[bytes, EncodingPayloadConverter]

    def __init__(self, *converters: EncodingPayloadConverter) -> None:
        self._set_converters(*converters)

    def _set_converters(self, *converters: EncodingPayloadConverter) -> None:
        self.converters = {c.encoding.encode(): c for c in converters}

    def to_payloads(self, values: Sequence[Any]) -> List[Payload]:
        payloads: List[Payload] = []
        for index, value in enumerate(values):
            payload: Optional[Payload] = None
            # RawValue passes straight through.
            if isinstance(value, RawValue):
                payload = value.payload
            else:
                # Attempted serially in case a stateful converter relies on
                # previous values.
                for converter in self.converters.values():
                    payload = converter.to_payload(value)
                    if payload is not None:
                        break
            if payload is None:
                raise RuntimeError(
                    f"Value at index {index} of type {type(value)} has no known converter"
                )
            payloads.append(payload)
        return payloads

    def from_payloads(
        self, payloads: Sequence[Payload], type_hints: Optional[List[type]] = None
    ) -> List[Any]:
        values: List[Any] = []
        hints = type_hints or []
        for index, (payload, type_hint) in enumerate(zip_longest(payloads, hints)):
            if type_hint == RawValue:
                values.append(RawValue(payload))
                continue
            encoding = payload.metadata.get("encoding", b"<unknown>")
            converter = self.converters.get(encoding)
            if converter is None:
                raise KeyError(f"Unknown payload encoding {encoding.decode()}")
            try:
                values.append(converter.from_payload(payload, type_hint))
            except RuntimeError as err:
                raise RuntimeError(
                    f"Payload at index {index} with encoding {encoding.decode()} "
                    f"could not be converted"
                ) from err
        return values

    def with_context(
        self, context: SerializationContext
    ) -> "CompositePayloadConverter":
        converters = self._converters_with_context(context)
        if converters is None:
            return self
        new_instance = type(self)()  # must have a nullary constructor
        new_instance._set_converters(*converters)
        return new_instance

    def _converters_with_context(
        self, context: SerializationContext
    ) -> Optional[List[EncodingPayloadConverter]]:
        if not self._any_converter_takes_context:
            return None
        converters: List[EncodingPayloadConverter] = []
        any_with_context = False
        for c in self.converters.values():
            if isinstance(c, WithSerializationContext):
                replaced = c.with_context(context)
                assert isinstance(replaced, EncodingPayloadConverter)
                converters.append(replaced)
                any_with_context |= id(replaced) != id(c)
            else:
                converters.append(c)
        return converters if any_with_context else None

    @functools.cached_property
    def _any_converter_takes_context(self) -> bool:
        return any(
            isinstance(c, WithSerializationContext) for c in self.converters.values()
        )


class BinaryNullPayloadConverter(EncodingPayloadConverter):
    """Converter for ``binary/null`` payloads supporting ``None``."""

    @property
    def encoding(self) -> str:
        return "binary/null"

    def to_payload(self, value: Any) -> Optional[Payload]:
        if value is None:
            return Payload(metadata={"encoding": self.encoding.encode()})
        return None

    def from_payload(self, payload: Payload, type_hint: Optional[type] = None) -> Any:
        if len(payload.data) > 0:
            raise RuntimeError("Expected empty data set for binary/null")
        return None


class BinaryPlainPayloadConverter(EncodingPayloadConverter):
    """Converter for ``binary/plain`` payloads supporting ``bytes``."""

    @property
    def encoding(self) -> str:
        return "binary/plain"

    def to_payload(self, value: Any) -> Optional[Payload]:
        if isinstance(value, bytes):
            return Payload(metadata={"encoding": self.encoding.encode()}, data=value)
        return None

    def from_payload(self, payload: Payload, type_hint: Optional[type] = None) -> Any:
        return payload.data


class AdvancedJSONEncoder(json.JSONEncoder):
    """JSON encoder supporting datetime, dataclasses, objects with a ``dict()``
    method, all iterables (as lists), and UUID."""

    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        dict_fn = getattr(o, "dict", None)
        if callable(dict_fn):
            return dict_fn()
        if not isinstance(o, list) and isinstance(o, collections.abc.Iterable):
            return list(o)
        if isinstance(o, uuid.UUID):
            return str(o)
        return super().default(o)


JSONTypeConverterUnhandled = NewType("JSONTypeConverterUnhandled", object)
"""Type of :py:attr:`JSONTypeConverter.Unhandled`."""


class JSONTypeConverter(ABC):
    """Converts a :py:func:`json.loads` result (scalar/list/dict) to a known
    type, for use as a custom converter in :py:func:`value_to_type`."""

    Unhandled: ClassVar[JSONTypeConverterUnhandled] = JSONTypeConverterUnhandled(
        object()
    )
    """Sentinel returned from :py:meth:`to_typed_value` to decline a value."""

    @abstractmethod
    def to_typed_value(
        self, hint: type, value: Any
    ) -> "Any | None | JSONTypeConverterUnhandled":
        """Convert ``value`` to ``hint`` or return :py:attr:`Unhandled`."""
        raise NotImplementedError


class JSONPlainPayloadConverter(EncodingPayloadConverter):
    """Converter for ``json/plain`` payloads supporting common Python values.

    Encoding accepts everything :py:func:`json.dump` does, plus dataclasses,
    objects with a ``dict()`` method, and iterables (via
    :py:class:`AdvancedJSONEncoder`). Decoding uses type hints to rebuild the
    original types (see :py:func:`value_to_type`)."""

    def __init__(
        self,
        *,
        encoder: Optional[Type[json.JSONEncoder]] = AdvancedJSONEncoder,
        decoder: Optional[Type[json.JSONDecoder]] = None,
        encoding: str = "json/plain",
        custom_type_converters: Sequence[JSONTypeConverter] = [],
    ) -> None:
        super().__init__()
        self._encoder = encoder
        self._decoder = decoder
        self._encoding = encoding
        self._custom_type_converters = custom_type_converters

    @property
    def encoding(self) -> str:
        return self._encoding

    def to_payload(self, value: Any) -> Optional[Payload]:
        # sort_keys keeps the bytes deterministic — load-bearing for replay.
        return Payload(
            metadata={"encoding": self._encoding.encode()},
            data=json.dumps(
                value, cls=self._encoder, separators=(",", ":"), sort_keys=True
            ).encode(),
        )

    def from_payload(self, payload: Payload, type_hint: Optional[type] = None) -> Any:
        try:
            obj = json.loads(payload.data, cls=self._decoder)
        except json.JSONDecodeError as err:
            raise RuntimeError("Failed parsing") from err
        if type_hint:
            obj = value_to_type(type_hint, obj, self._custom_type_converters)
        return obj


class DefaultPayloadConverter(CompositePayloadConverter):
    """Default payload converter: ``None``, ``bytes``, and anything
    :py:func:`json.dump` accepts. Available as
    :py:attr:`PayloadConverter.default`."""

    default_encoding_payload_converters: ClassVar[tuple[EncodingPayloadConverter, ...]]
    """The ordered encoders used by the default converter (JSON plain must stay
    last — it throws on unknown types instead of declining)."""

    def __init__(self) -> None:
        super().__init__(*DefaultPayloadConverter.default_encoding_payload_converters)


def _get_iso_datetime_parser() -> Callable[[str], datetime]:
    """The ISO-8601 datetime parser for this interpreter (mirrors temporalio):
    ``datetime.fromisoformat`` on 3.11+, ``dateutil.isoparse`` on older
    versions where ``fromisoformat`` rejects valid ISO-8601 strings (a trailing
    "Z", basic format, …). The dependency is only installed for python <3.11."""
    if sys.version_info >= (3, 11):
        return datetime.fromisoformat
    from dateutil import parser

    # dateutil ships no stubs, so isoparse is Any; cast to keep mypy --strict
    # happy on <3.11 (this branch is unreachable, hence cast-free, on 3.11+).
    return cast(Callable[[str], datetime], parser.isoparse)


def value_to_type(
    hint: type,
    value: Any,
    custom_converters: Sequence[JSONTypeConverter] = [],
) -> Any:
    """Convert a raw JSON-loaded ``value`` to ``hint``.

    Handles primitives, ``datetime``, ``NewType``, ``Literal``, ``Union`` /
    ``Optional``, mappings/``TypedDict``, dataclasses, Pydantic v1 models,
    ``IntEnum`` / ``StrEnum``, ``UUID``, and iterables (list/tuple/set/deque).
    Raises ``TypeError`` if it cannot.
    """
    # Custom converters first.
    for conv in custom_converters:
        ret = conv.to_typed_value(hint, value)
        if ret is not JSONTypeConverter.Unhandled:
            return ret

    # Any or primitives.
    if hint is Any:
        return value
    elif hint is datetime:
        if isinstance(value, str):
            try:
                return _get_iso_datetime_parser()(value)
            except ValueError as err:
                raise TypeError(f"Failed parsing datetime string: {value}") from err
        elif isinstance(value, datetime):
            return value
        raise TypeError(f"Expected datetime or ISO8601 string, got {type(value)}")
    elif hint is int or hint is float:
        if not isinstance(value, (int, float)):
            raise TypeError(f"Expected value to be int|float, was {type(value)}")
        return hint(value)
    elif hint is bool:
        if not isinstance(value, bool):
            raise TypeError(f"Expected value to be bool, was {type(value)}")
        return bool(value)
    elif hint is str:
        if not isinstance(value, str):
            raise TypeError(f"Expected value to be str, was {type(value)}")
        return str(value)
    elif hint is bytes:
        if not isinstance(value, (str, bytes, list)):
            raise TypeError(f"Expected value to be bytes, was {type(value)}")
        # Other SDKs serialize bytes as base64 strings; in Python it's a
        # numeric array (see AdvancedJSONEncoder).
        return bytes(value)  # type: ignore[arg-type]
    elif hint is type(None):
        if value is not None:
            raise TypeError(f"Expected None, got value of type {type(value)}")
        return None

    # NewType (only a class since 3.10, so detect by supertype presence).
    supertype = getattr(hint, "__supertype__", None)
    if supertype:
        return value_to_type(supertype, value, custom_converters)

    origin = getattr(hint, "__origin__", hint)
    type_args: tuple[Any, ...] = getattr(hint, "__args__", ())

    # Literal.
    if origin is Literal:
        if value not in type_args:
            raise TypeError(f"Value {value} not in literal values {type_args}")
        return value

    # Union (Optional is Union[..., None]).
    if origin is typing.Union or isinstance(origin, UnionType):
        for arg in type_args:
            try:
                return value_to_type(arg, value, custom_converters)
            except Exception:
                pass
        raise TypeError(f"Failed converting to {hint} from {value}")

    # Mapping.
    if inspect.isclass(origin) and issubclass(origin, collections.abc.Mapping):
        if not isinstance(value, collections.abc.Mapping):
            raise TypeError(f"Expected {hint}, value was {type(value)}")
        ret_dict = {}
        # Required/optional keys means a TypedDict, so per-key types apply.
        per_key_types: Optional[dict[str, type]] = None
        if getattr(origin, "__required_keys__", None) or getattr(
            origin, "__optional_keys__", None
        ):
            per_key_types = get_type_hints(origin)
        key_type = (
            type_args[0]
            if len(type_args) > 0
            and type_args[0] is not Any
            and not isinstance(type_args[0], TypeVar)
            else None
        )
        value_type = (
            type_args[1]
            if len(type_args) > 1
            and type_args[1] is not Any
            and not isinstance(type_args[1], TypeVar)
            else None
        )
        for key, item in value.items():
            this_value_type = value_type
            if per_key_types:
                this_value_type = per_key_types.get(key)
            if key_type:
                # JSON only supports str/int/float/bool/None keys, serializing
                # all to strings; recover the original type from the hint.
                try:
                    if isinstance(key, str):
                        if key_type is int or key_type is float:
                            key = key_type(key)
                        elif key_type is bool:
                            key = {"true": True, "false": False}[key]
                        elif key_type is type(None):
                            key = {"null": None}[key]
                    if not isinstance(key_type, type) or not isinstance(key, key_type):
                        key = value_to_type(key_type, key, custom_converters)
                except Exception as err:
                    raise TypeError(
                        f"Failed converting key {key!r} to type {key_type} in "
                        f"mapping {hint}"
                    ) from err
            if this_value_type:
                try:
                    item = value_to_type(this_value_type, item, custom_converters)
                except Exception as err:
                    raise TypeError(
                        f"Failed converting value for key {key!r} in mapping {hint}"
                    ) from err
            ret_dict[key] = item
        # A TypedDict: instantiate to get its validation.
        if per_key_types:
            return hint(**ret_dict)
        return ret_dict

    # Dataclass.
    if dataclasses.is_dataclass(hint):
        if not isinstance(value, dict):
            raise TypeError(
                f"Cannot convert to dataclass {hint}, value is {type(value)} not dict"
            )
        # Unknown fields are silently ignored; a missing required field fails
        # at instantiation.
        fields = dataclasses.fields(hint)
        field_hints = get_type_hints(hint)
        field_values = {}
        for fld in fields:
            field_value = value.get(fld.name, dataclasses.MISSING)
            if field_value is not dataclasses.MISSING:
                try:
                    field_values[fld.name] = value_to_type(
                        field_hints[fld.name], field_value, custom_converters
                    )
                except Exception as err:
                    raise TypeError(
                        f"Failed converting field {fld.name} on dataclass {hint}"
                    ) from err
        return hint(**field_values)

    # Pydantic v1 model (``parse_obj``). Pydantic v2 models are not specially
    # supported by the default converter (no ``contrib.pydantic`` — configure a
    # custom ``DataConverter`` for them; DESIGN §6.9).
    parse_obj_attr = inspect.getattr_static(hint, "parse_obj", None)
    if isinstance(parse_obj_attr, (classmethod, staticmethod)):
        if not isinstance(value, dict):
            raise TypeError(
                f"Cannot convert to {hint}, value is {type(value)} not dict"
            )
        return getattr(hint, "parse_obj")(value)

    # IntEnum.
    if inspect.isclass(hint) and issubclass(hint, IntEnum):
        if not isinstance(value, int):
            raise TypeError(
                f"Cannot convert to enum {hint}, value not an integer, value is "
                f"{type(value)}"
            )
        return hint(value)

    # StrEnum (3.11+).
    if sys.version_info >= (3, 11):
        if inspect.isclass(hint) and issubclass(hint, StrEnum):
            if not isinstance(value, str):
                raise TypeError(
                    f"Cannot convert to enum {hint}, value not a string, value is "
                    f"{type(value)}"
                )
            return hint(value)

    # UUID.
    if inspect.isclass(hint) and issubclass(hint, uuid.UUID):
        return hint(value)

    # Iterable — last, as it catches several others.
    if inspect.isclass(origin) and issubclass(origin, collections.abc.Iterable):
        if not isinstance(value, collections.abc.Iterable):
            raise TypeError(f"Expected {hint}, value was {type(value)}")
        ret_list = []
        if not type_args or (
            len(type_args) == 1
            and (isinstance(type_args[0], TypeVar) or type_args[0] is Ellipsis)
        ):
            ret_list = list(value)
        else:
            for i, item in enumerate(value):
                # Non-tuples use the first arg; tuples use the per-position arg
                # (or the one before an ellipsis).
                if origin is not tuple:
                    arg_type = type_args[0]
                elif len(type_args) > i and type_args[i] is not Ellipsis:
                    arg_type = type_args[i]
                elif type_args[-1] is Ellipsis:
                    arg_type = type_args[-2]
                else:
                    raise TypeError(
                        f"Type {hint} only expecting {len(type_args)} values, got at "
                        f"least {i + 1}"
                    )
                try:
                    ret_list.append(value_to_type(arg_type, item, custom_converters))
                except Exception as err:
                    raise TypeError(f"Failed converting {hint} index {i}") from err
        if origin is tuple:
            return tuple(ret_list)
        elif origin is set:
            return set(ret_list)
        elif origin is collections.deque:
            return collections.deque(ret_list)
        return ret_list

    raise TypeError(f"Unserializable type during conversion: {hint}")


# ---------------------------------------------------------------------------
# Payload codec.
# ---------------------------------------------------------------------------
class PayloadCodec(ABC):
    """Codec for encoding/decoding payload *bytes* — compression or
    encryption. Runs after payload conversion (encode) and before it
    (decode)."""

    @abstractmethod
    async def encode(self, payloads: Sequence[Payload]) -> List[Payload]:
        """Encode payloads. ``payloads`` must not be mutated."""
        raise NotImplementedError

    @abstractmethod
    async def decode(self, payloads: Sequence[Payload]) -> List[Payload]:
        """Decode payloads. ``payloads`` must not be mutated."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Failure converter.
#
# temporalio's converts to/from a protobuf ``Failure`` in place; we have no
# such proto, so ours converts to/from the failure-envelope dict that
# ``_internal.payloads`` already defines (DELIBERATE_DEVIATION). The payload
# converter routes embedded user values (details, heartbeat details); detail
# encoding lands when the converter is wired through the interpreter.
# ---------------------------------------------------------------------------
Failure = Dict[str, Any]


class FailureConverter(ABC):
    """Converts exceptions to/from the serialized failure envelope."""

    default: ClassVar["FailureConverter"]

    @abstractmethod
    def to_failure(
        self, exception: BaseException, payload_converter: PayloadConverter
    ) -> Failure:
        """Serialize ``exception`` to a failure envelope."""
        raise NotImplementedError

    @abstractmethod
    def from_failure(
        self, failure: Failure, payload_converter: PayloadConverter
    ) -> BaseException:
        """Reconstruct an exception from a failure envelope."""
        raise NotImplementedError


class DefaultFailureConverter(FailureConverter):
    """Default failure converter, bridging the Temporal exception tree to the
    stable failure envelope (``_internal.payloads``)."""

    def __init__(self, *, encode_common_attributes: bool = False) -> None:
        self._encode_common_attributes = encode_common_attributes

    def to_failure(
        self, exception: BaseException, payload_converter: PayloadConverter
    ) -> Failure:
        from ._internal.payloads import serialize_failure

        return serialize_failure(exception)

    def from_failure(
        self, failure: Failure, payload_converter: PayloadConverter
    ) -> BaseException:
        from ._internal.payloads import deserialize_failure

        return deserialize_failure(failure)


class DefaultFailureConverterWithEncodedAttributes(DefaultFailureConverter):
    """Default failure converter that moves message/stack trace into an
    encoded attribute (so a codec can encrypt them)."""

    def __init__(self) -> None:
        super().__init__(encode_common_attributes=True)


# ---------------------------------------------------------------------------
# DataConverter — the top-level orchestrator.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DataConverter:
    """Combines a :py:class:`PayloadConverter` (values ↔ payloads) with an
    optional :py:class:`PayloadCodec` (payload bytes ↔ bytes) and a
    :py:class:`FailureConverter`."""

    payload_converter_class: Type[PayloadConverter] = DefaultPayloadConverter
    """Class to instantiate for payload conversion."""

    payload_codec: Optional[PayloadCodec] = None
    """Optional codec for encoding payload bytes."""

    failure_converter_class: Type[FailureConverter] = DefaultFailureConverter
    """Class to instantiate for failure conversion."""

    payload_converter: PayloadConverter = field(init=False)
    """Instance built from :py:attr:`payload_converter_class`."""

    failure_converter: FailureConverter = field(init=False)
    """Instance built from :py:attr:`failure_converter_class`."""

    default: ClassVar["DataConverter"]
    """Singleton default data converter."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload_converter", self.payload_converter_class())
        object.__setattr__(self, "failure_converter", self.failure_converter_class())

    async def encode(self, values: Sequence[Any]) -> List[Payload]:
        """Convert values to payloads, then codec-encode the bytes."""
        payloads = self.payload_converter.to_payloads(values)
        if self.payload_codec:
            payloads = await self.payload_codec.encode(payloads)
        return payloads

    async def decode(
        self, payloads: Sequence[Payload], type_hints: Optional[List[type]] = None
    ) -> List[Any]:
        """Codec-decode the bytes, then convert payloads to values."""
        if self.payload_codec:
            payloads = await self.payload_codec.decode(list(payloads))
        return self.payload_converter.from_payloads(payloads, type_hints)

    async def encode_failure(self, exception: BaseException) -> Failure:
        """Convert (and, later, codec-encode) an exception to a failure
        envelope."""
        return self.failure_converter.to_failure(exception, self.payload_converter)

    async def decode_failure(self, failure: Failure) -> BaseException:
        """Reconstruct an exception from a failure envelope."""
        return self.failure_converter.from_failure(failure, self.payload_converter)

    def with_context(self, context: SerializationContext) -> "DataConverter":
        """Return an instance with ``context`` set on the component converters
        that consume it (or ``self`` if none do)."""
        payload_converter = self.payload_converter
        payload_codec = self.payload_codec
        failure_converter = self.failure_converter
        if isinstance(payload_converter, WithSerializationContext):
            replaced = payload_converter.with_context(context)
            assert isinstance(replaced, PayloadConverter)
            payload_converter = replaced
        if isinstance(payload_codec, WithSerializationContext):
            replaced_codec = payload_codec.with_context(context)
            assert isinstance(replaced_codec, PayloadCodec)
            payload_codec = replaced_codec
        if isinstance(failure_converter, WithSerializationContext):
            replaced_fc = failure_converter.with_context(context)
            assert isinstance(replaced_fc, FailureConverter)
            failure_converter = replaced_fc
        if (
            payload_converter is self.payload_converter
            and payload_codec is self.payload_codec
            and failure_converter is self.failure_converter
        ):
            return self
        cloned = dataclasses.replace(self)
        object.__setattr__(cloned, "payload_converter", payload_converter)
        object.__setattr__(cloned, "payload_codec", payload_codec)
        object.__setattr__(cloned, "failure_converter", failure_converter)
        return cloned


def default() -> DataConverter:
    """The default data converter.

    .. deprecated:: use :py:attr:`DataConverter.default`.
    """
    return DataConverter.default


# Set up after all converter classes are defined (forward-reference safe).
DefaultPayloadConverter.default_encoding_payload_converters = (
    BinaryNullPayloadConverter(),
    BinaryPlainPayloadConverter(),
    JSONPlainPayloadConverter(),  # must remain last — it throws on unknown types
)

DataConverter.default = DataConverter()
PayloadConverter.default = DataConverter.default.payload_converter
FailureConverter.default = DataConverter.default.failure_converter
