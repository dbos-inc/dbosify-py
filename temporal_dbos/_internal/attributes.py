"""Memo and search-attribute conversion, backed by DBOS native workflow
attributes (DESIGN §6.2).

Temporal has two metadata namespaces — ``memo`` (opaque, converter-encoded,
codec-capable) and ``search_attributes`` (typed, indexed, queryable). DBOS
gives each workflow a single JSON ``attributes`` dict (a ``JSONB`` column with a
GIN index, filterable via ``@>`` containment). We map both Temporal namespaces
into that one dict under reserved keys::

    {
      "memo": {name: <tagged-payload-dict>, ...},
      "search_attributes": {name: {"t": <metadata-type>, "v": <json-scalar>}, ...}
    }

Memo values round-trip through the active :class:`DataConverter` (so a
``PayloadCodec`` can encrypt them); search-attribute values are stored as plain
JSON scalars so the JSONB index can see them. Everything here must stay
``json.dumps``-able with the default encoder — DBOS validates that before it
records the status.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from ..common import (
    SearchAttributeIndexedValueType,
    SearchAttributeKey,
    SearchAttributePair,
    SearchAttributes,
    SearchAttributeUpdate,
    TypedSearchAttributes,
)
from . import conversion

MEMO_KEY = "memo"
SEARCH_ATTRIBUTES_KEY = "search_attributes"

# A search attribute value the way it sits in either Temporal's typed or untyped
# form, before/after we flatten it to a JSON scalar.
SearchAttributeInput = Union[TypedSearchAttributes, SearchAttributes]


# --- search attributes: typed <-> JSON-scalar --------------------------------


def _sa_value_to_json(key: SearchAttributeKey[Any], value: Any) -> Any:
    """Flatten a typed search-attribute value to a JSON scalar."""
    t = key.indexed_value_type
    if t == SearchAttributeIndexedValueType.DATETIME:
        assert isinstance(value, datetime)
        return value.isoformat()
    if t == SearchAttributeIndexedValueType.KEYWORD_LIST:
        return list(value)
    return value


def _sa_value_from_json(key: SearchAttributeKey[Any], jv: Any) -> Any:
    """Rebuild a typed search-attribute value from its JSON scalar."""
    t = key.indexed_value_type
    if t == SearchAttributeIndexedValueType.DATETIME:
        return datetime.fromisoformat(jv)
    if t == SearchAttributeIndexedValueType.KEYWORD_LIST:
        return list(jv)
    return jv


def _typed_pairs(attrs: SearchAttributeInput) -> List[SearchAttributePair[Any]]:
    """Normalize either SA form to a list of typed pairs (guessing the key type
    for the deprecated untyped dict form, as temporalio does)."""
    if isinstance(attrs, TypedSearchAttributes):
        return list(attrs.search_attributes)
    pairs: List[SearchAttributePair[Any]] = []
    for name, values in attrs.items():
        key = SearchAttributeKey._guess_from_untyped_values(name, values)
        if key is None:
            continue
        if key.indexed_value_type == SearchAttributeIndexedValueType.KEYWORD_LIST:
            value: Any = list(values)
        else:
            value = list(values)[0]
        pairs.append(SearchAttributePair(key=key, value=value))
    return pairs


def encode_search_attributes(attrs: SearchAttributeInput) -> Dict[str, Any]:
    """The stored ``{name: {"t": metadata_type, "v": json_scalar}}`` map."""
    out: Dict[str, Any] = {}
    for pair in _typed_pairs(attrs):
        out[pair.key.name] = {
            "t": pair.key._metadata_type,
            "v": _sa_value_to_json(pair.key, pair.value),
        }
    return out


def decode_search_attributes(stored: Mapping[str, Any]) -> TypedSearchAttributes:
    pairs: List[SearchAttributePair[Any]] = []
    for name, entry in stored.items():
        key = SearchAttributeKey._from_metadata_type(name, str(entry["t"]))
        if key is None:
            continue
        pairs.append(
            SearchAttributePair(key=key, value=_sa_value_from_json(key, entry["v"]))
        )
    return TypedSearchAttributes(pairs)


def typed_to_untyped(typed: TypedSearchAttributes) -> Dict[str, List[Any]]:
    """The deprecated untyped ``{name: [values]}`` view (for the legacy
    ``search_attributes`` property on info/describe)."""
    out: Dict[str, List[Any]] = {}
    for pair in typed.search_attributes:
        if pair.key.indexed_value_type == SearchAttributeIndexedValueType.KEYWORD_LIST:
            out[pair.key.name] = list(pair.value)
        else:
            out[pair.key.name] = [pair.value]
    return out


def apply_sa_updates(
    current: TypedSearchAttributes,
    updates: Union[SearchAttributes, Sequence[SearchAttributeUpdate[Any]]],
) -> TypedSearchAttributes:
    """Upsert semantics for ``workflow.upsert_search_attributes`` — set/replace
    keys, ``value_unset`` removes them. Accepts the typed update sequence or the
    deprecated untyped dict."""
    if isinstance(updates, Mapping):
        return current.updated(*_typed_pairs(updates))
    result = current
    for update in updates:
        if update.value is None:
            kept = [
                p for p in result.search_attributes if p.key.name != update.key.name
            ]
            result = TypedSearchAttributes(kept)
        else:
            result = result.updated(
                SearchAttributePair(key=update.key, value=update.value)
            )
    return result


# --- memo: converter-encoded per key -----------------------------------------


async def encode_memo(memo: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: await conversion.encode_value(v) for k, v in memo.items()}


async def decode_memo(stored: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: await conversion.decode_value(v) for k, v in stored.items()}


def apply_memo_updates(
    current: Dict[str, Any], updates: Mapping[str, Any]
) -> Dict[str, Any]:
    """Upsert semantics for ``workflow.upsert_memo`` over already-encoded memo:
    a ``None`` value removes the key, others set it. ``updates`` values are the
    already-encoded payload dicts."""
    result = dict(current)
    for name, value in updates.items():
        if value is None:
            result.pop(name, None)
        else:
            result[name] = value
    return result


# --- the combined DBOS attributes dict ---------------------------------------


async def encode_attributes(
    memo: Optional[Mapping[str, Any]],
    search_attributes: Optional[SearchAttributeInput],
) -> Optional[Dict[str, Any]]:
    """Build the namespaced DBOS attributes dict, or ``None`` when both
    namespaces are empty (so workflows without metadata keep a null column)."""
    out: Dict[str, Any] = {}
    if memo:
        out[MEMO_KEY] = await encode_memo(memo)
    if search_attributes is not None:
        encoded_sa = encode_search_attributes(search_attributes)
        if encoded_sa:
            out[SEARCH_ATTRIBUTES_KEY] = encoded_sa
    return out or None


async def decode_attributes(
    attrs: Optional[Mapping[str, Any]],
) -> Tuple[Dict[str, Any], TypedSearchAttributes]:
    """Split a stored DBOS attributes dict back into (decoded memo, typed search
    attributes)."""
    if not attrs:
        return {}, TypedSearchAttributes.empty
    memo = await decode_memo(attrs.get(MEMO_KEY, {}))
    sa = decode_search_attributes(attrs.get(SEARCH_ATTRIBUTES_KEY, {}))
    return memo, sa
