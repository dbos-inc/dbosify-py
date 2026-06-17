"""Common types and enums, mirroring ``temporalio.common``.

Phase 0 carries only what the interpreter needs (RetryPolicy and the
workflow-ID policy enums); the rest of the module lands with the client
facade in Phase 1. Search-attribute types land with memo/search-attribute
storage in Phase 3.
"""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Collection,
    Generic,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
    get_origin,
    overload,
)

# typing.NamedTuple cannot be combined with Generic before Python 3.11
# ("Multiple inheritance with NamedTuple is not supported"). typing_extensions
# backports the 3.11 generic NamedTuple to our 3.10 floor — same approach as
# temporalio, which imports NamedTuple from typing_extensions for this reason.
from typing_extensions import NamedTuple

if TYPE_CHECKING:
    from .converter import Payload

__all__ = [
    "AutoUpgradeVersioningOverride",
    "PinnedVersioningOverride",
    "Priority",
    "QueryRejectCondition",
    "RawValue",
    "RetryPolicy",
    "SearchAttributeIndexedValueType",
    "SearchAttributeKey",
    "SearchAttributePair",
    "SearchAttributeUpdate",
    "SearchAttributeValue",
    "SearchAttributeValues",
    "SearchAttributes",
    "TypedSearchAttributes",
    "VersioningBehavior",
    "VersioningOverride",
    "WorkerDeploymentVersion",
    "WorkflowIDReusePolicy",
    "WorkflowIDConflictPolicy",
]


@dataclass(frozen=True)
class RawValue:
    """Representation of an unconverted, raw payload, mirroring
    ``temporalio.common.RawValue``.

    Use as a parameter or return type in workflows, activities, signals, and
    queries to pass a payload through without conversion; the system still
    encodes/decodes the payload bytes.
    """

    payload: "Payload"


@dataclass(frozen=True)
class Priority:
    """Priority metadata controlling relative task-processing order, mirroring
    ``temporalio.common.Priority``.

    temporal-dbos does not implement priority-based dispatch (DBOS queues are
    FIFO), so this type exists for signature parity and for the default that
    ``workflow.info().priority`` / ``activity.info().priority`` return —
    temporalio specifies an unset priority surfaces as the default instance,
    which is exactly what we return.
    """

    priority_key: Optional[int] = None
    """A positive integer (1..n); smaller is higher priority. Default unset."""

    fairness_key: Optional[str] = None
    """A short string keying a fairness-balancing mechanism. Default unset."""

    fairness_weight: Optional[float] = None
    """Weight for fairness dispatch within a ``fairness_key``. Default unset."""

    default: ClassVar["Priority"]
    """Singleton default priority instance."""

    def __post_init__(self) -> None:
        if self.priority_key is not None:
            if not isinstance(self.priority_key, int):
                raise TypeError("priority_key must be an integer")
            if self.priority_key < 1:
                raise ValueError("priority_key must be a positive integer")


Priority.default = Priority(priority_key=None, fairness_key=None, fairness_weight=None)


@dataclass(frozen=True)
class RetryPolicy:
    """Options for retrying workflows and activities."""

    initial_interval: timedelta = timedelta(seconds=1)
    """Backoff interval for the first retry. Default 1s."""

    backoff_coefficient: float = 2.0
    """Coefficient to multiply previous backoff interval by to get new
    interval. Default 2.0.
    """

    maximum_interval: Optional[timedelta] = None
    """Maximum backoff interval between retries. Default 100x
    :py:attr:`initial_interval`.
    """

    maximum_attempts: int = 0
    """Maximum number of attempts.

    If 0, the default, there is no maximum.
    """

    non_retryable_error_types: Optional[Sequence[str]] = None
    """List of error types that are not retryable."""

    def _validate(self) -> None:
        # Validation taken from the Temporal Go SDK's test suite, mirroring
        # temporalio's RetryPolicy._validate.
        if self.maximum_attempts == 1:
            # Ignore other validation if disabling retries
            return
        if self.initial_interval.total_seconds() < 0:
            raise ValueError("Initial interval cannot be negative")
        if self.backoff_coefficient < 1:
            raise ValueError("Backoff coefficient cannot be less than 1")
        if self.maximum_interval:
            if self.maximum_interval.total_seconds() < 0:
                raise ValueError("Maximum interval cannot be negative")
            if self.maximum_interval < self.initial_interval:
                raise ValueError(
                    "Maximum interval cannot be less than initial interval"
                )
        if self.maximum_attempts < 0:
            raise ValueError("Maximum attempts cannot be negative")


class QueryRejectCondition(IntEnum):
    """When a query should be rejected based on workflow status, mirroring
    ``temporalio.common.QueryRejectCondition``."""

    NONE = 1
    NOT_OPEN = 2
    NOT_COMPLETED_CLEANLY = 3


class WorkflowIDReusePolicy(IntEnum):
    """How already-in-use workflow IDs are handled on start."""

    ALLOW_DUPLICATE = 1
    ALLOW_DUPLICATE_FAILED_ONLY = 2
    REJECT_DUPLICATE = 3
    TERMINATE_IF_RUNNING = 4


class WorkflowIDConflictPolicy(IntEnum):
    """How already-running workflows of the same ID are handled on start."""

    UNSPECIFIED = 0
    FAIL = 1
    USE_EXISTING = 2
    TERMINATE_EXISTING = 3


# --- Search attributes (mirroring ``temporalio.common``) --------------------
#
# These mirror the Temporal SDK's typed search-attribute surface verbatim
# (signatures, factory methods, deprecation of the untyped dict form). The one
# self-contained departure: the indexed-value-type ints are inlined here rather
# than pulled from ``temporalio.api.enums.v1.IndexedValueType`` (we never depend
# on ``temporalio`` at runtime); the values match that protobuf enum exactly.

# A list so we can catch callers accidentally passing a bare ``str`` (which is
# itself a Sequence) instead of a list of values.
SearchAttributeValues = Union[
    "list[str]", "list[int]", "list[float]", "list[bool]", "list[datetime]"
]

SearchAttributes = Mapping[str, SearchAttributeValues]

SearchAttributeValue = Union[str, int, float, bool, datetime, Sequence[str]]

SearchAttributeValueType = TypeVar(
    "SearchAttributeValueType", str, int, float, bool, datetime, Sequence[str]
)

_DefaultT = TypeVar("_DefaultT")


class SearchAttributeIndexedValueType(IntEnum):
    """Server index type of a search attribute.

    Values match ``temporalio.api.enums.v1.IndexedValueType``.
    """

    TEXT = 1
    KEYWORD = 2
    INT = 3
    DOUBLE = 4
    BOOL = 5
    DATETIME = 6
    KEYWORD_LIST = 7


class SearchAttributeKey(ABC, Generic[SearchAttributeValueType]):
    """Typed search attribute key representation.

    Use one of the ``for_*`` static methods here to create a key.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of the key."""
        ...

    @property
    @abstractmethod
    def indexed_value_type(self) -> SearchAttributeIndexedValueType:
        """Get the server index type of the key."""
        ...

    @property
    @abstractmethod
    def value_type(self) -> type[SearchAttributeValueType]:
        """Get the Python type of value for the key.

        This may contain generics which cannot be used in ``isinstance``.
        :py:attr:`origin_value_type` can be used instead.
        """
        ...

    @property
    def origin_value_type(self) -> type:
        """Get the Python type of value for the key without generics."""
        return get_origin(self.value_type) or self.value_type

    @property
    def _metadata_type(self) -> str:
        index_type = self.indexed_value_type
        if index_type == SearchAttributeIndexedValueType.TEXT:
            return "Text"
        elif index_type == SearchAttributeIndexedValueType.KEYWORD:
            return "Keyword"
        elif index_type == SearchAttributeIndexedValueType.INT:
            return "Int"
        elif index_type == SearchAttributeIndexedValueType.DOUBLE:
            return "Double"
        elif index_type == SearchAttributeIndexedValueType.BOOL:
            return "Bool"
        elif index_type == SearchAttributeIndexedValueType.DATETIME:
            return "Datetime"
        elif index_type == SearchAttributeIndexedValueType.KEYWORD_LIST:
            return "KeywordList"
        raise ValueError(f"Unrecognized type: {self}")

    def value_set(
        self, value: SearchAttributeValueType
    ) -> SearchAttributeUpdate[SearchAttributeValueType]:
        """Create a search attribute update to set the given value on this key."""
        return _SearchAttributeUpdate[SearchAttributeValueType](self, value)

    def value_unset(self) -> SearchAttributeUpdate[SearchAttributeValueType]:
        """Create a search attribute update to unset the value on this key."""
        return _SearchAttributeUpdate[SearchAttributeValueType](self, None)

    @staticmethod
    def for_text(name: str) -> SearchAttributeKey[str]:
        """Create a 'Text' search attribute type."""
        return _SearchAttributeKey[str](name, SearchAttributeIndexedValueType.TEXT, str)

    @staticmethod
    def for_keyword(name: str) -> SearchAttributeKey[str]:
        """Create a 'Keyword' search attribute type."""
        return _SearchAttributeKey[str](
            name, SearchAttributeIndexedValueType.KEYWORD, str
        )

    @staticmethod
    def for_int(name: str) -> SearchAttributeKey[int]:
        """Create an 'Int' search attribute type."""
        return _SearchAttributeKey[int](name, SearchAttributeIndexedValueType.INT, int)

    @staticmethod
    def for_float(name: str) -> SearchAttributeKey[float]:
        """Create a 'Double' search attribute type."""
        return _SearchAttributeKey[float](
            name, SearchAttributeIndexedValueType.DOUBLE, float
        )

    @staticmethod
    def for_bool(name: str) -> SearchAttributeKey[bool]:
        """Create a 'Bool' search attribute type."""
        return _SearchAttributeKey[bool](
            name, SearchAttributeIndexedValueType.BOOL, bool
        )

    @staticmethod
    def for_datetime(name: str) -> SearchAttributeKey[datetime]:
        """Create a 'Datetime' search attribute type."""
        return _SearchAttributeKey[datetime](
            name, SearchAttributeIndexedValueType.DATETIME, datetime
        )

    @staticmethod
    def for_keyword_list(name: str) -> SearchAttributeKey[Sequence[str]]:
        """Create a 'KeywordList' search attribute type."""
        return _SearchAttributeKey[Sequence[str]](
            name,
            SearchAttributeIndexedValueType.KEYWORD_LIST,
            # Generic types not supported yet like this:
            # https://github.com/python/mypy/issues/4717
            Sequence[str],  # type: ignore[type-abstract]
        )

    @staticmethod
    def _from_metadata_type(
        name: str, metadata_type: str
    ) -> Optional[SearchAttributeKey[Any]]:
        # The type metadata is usually in PascalCase (e.g. "KeywordList") but in
        # rare cases may be in SCREAMING_SNAKE_CASE.
        if metadata_type in ("Text", "INDEXED_VALUE_TYPE_TEXT"):
            return SearchAttributeKey.for_text(name)
        elif metadata_type in ("Keyword", "INDEXED_VALUE_TYPE_KEYWORD"):
            return SearchAttributeKey.for_keyword(name)
        elif metadata_type in ("Int", "INDEXED_VALUE_TYPE_INT"):
            return SearchAttributeKey.for_int(name)
        elif metadata_type in ("Double", "INDEXED_VALUE_TYPE_DOUBLE"):
            return SearchAttributeKey.for_float(name)
        elif metadata_type in ("Bool", "INDEXED_VALUE_TYPE_BOOL"):
            return SearchAttributeKey.for_bool(name)
        elif metadata_type in ("Datetime", "INDEXED_VALUE_TYPE_DATETIME"):
            return SearchAttributeKey.for_datetime(name)
        elif metadata_type in ("KeywordList", "INDEXED_VALUE_TYPE_KEYWORD_LIST"):
            return SearchAttributeKey.for_keyword_list(name)
        return None

    @staticmethod
    def _guess_from_untyped_values(
        name: str, vals: SearchAttributeValues
    ) -> Optional[SearchAttributeKey[Any]]:
        if not vals:
            return None
        elif len(vals) > 1:
            if isinstance(vals[0], str):
                return SearchAttributeKey.for_keyword_list(name)
        elif isinstance(vals[0], str):
            return SearchAttributeKey.for_keyword(name)
        # int is checked before bool, verbatim from temporalio: bool is an int
        # subclass, so an untyped bool value guesses to for_int (the for_bool
        # branch is effectively dead, but kept to mirror temporalio exactly).
        elif isinstance(vals[0], int):
            return SearchAttributeKey.for_int(name)
        elif isinstance(vals[0], float):
            return SearchAttributeKey.for_float(name)
        elif isinstance(vals[0], bool):
            return SearchAttributeKey.for_bool(name)
        elif isinstance(vals[0], datetime):
            return SearchAttributeKey.for_datetime(name)
        return None


@dataclass(frozen=True)
class _SearchAttributeKey(SearchAttributeKey[SearchAttributeValueType]):
    _name: str
    _indexed_value_type: SearchAttributeIndexedValueType
    # No supported way in Python to derive this, so we set it manually.
    _value_type: type[SearchAttributeValueType]

    @property
    def name(self) -> str:
        return self._name

    @property
    def indexed_value_type(self) -> SearchAttributeIndexedValueType:
        return self._indexed_value_type

    @property
    def value_type(self) -> type[SearchAttributeValueType]:
        return self._value_type


class SearchAttributePair(NamedTuple, Generic[SearchAttributeValueType]):
    """A named tuple representing a key/value search attribute pair."""

    key: SearchAttributeKey[SearchAttributeValueType]
    value: SearchAttributeValueType


class SearchAttributeUpdate(ABC, Generic[SearchAttributeValueType]):
    """Representation of a search attribute update."""

    @property
    @abstractmethod
    def key(self) -> SearchAttributeKey[SearchAttributeValueType]:
        """Key that is being set."""
        ...

    @property
    @abstractmethod
    def value(self) -> Optional[SearchAttributeValueType]:
        """Value that is being set or ``None`` if being unset."""
        ...


@dataclass(frozen=True)
class _SearchAttributeUpdate(SearchAttributeUpdate[SearchAttributeValueType]):
    _key: SearchAttributeKey[SearchAttributeValueType]
    _value: Optional[SearchAttributeValueType]

    @property
    def key(self) -> SearchAttributeKey[SearchAttributeValueType]:
        return self._key

    @property
    def value(self) -> Optional[SearchAttributeValueType]:
        return self._value


@dataclass(frozen=True)
class TypedSearchAttributes(Collection[SearchAttributePair[Any]]):
    """Collection of typed search attributes.

    This is represented as an immutable collection of
    :py:class:`SearchAttributePair`. This can be created passing a sequence of
    pairs to the constructor.
    """

    search_attributes: Sequence[SearchAttributePair[Any]]
    """Underlying sequence of search attribute pairs. Do not mutate this, only
    create new ``TypedSearchAttributes`` instances.

    These are sorted by key name during construction. Duplicates cannot exist.
    """

    empty: ClassVar[TypedSearchAttributes]
    """Class variable representing an empty set of attributes."""

    def __post_init__(self) -> None:
        # Sort by key name.
        object.__setattr__(
            self,
            "search_attributes",
            sorted(self.search_attributes, key=lambda pair: pair.key.name),
        )
        # Ensure no duplicates.
        for i, pair in enumerate(self.search_attributes):
            if i > 0 and self.search_attributes[i - 1].key.name == pair.key.name:
                raise ValueError(
                    f"Duplicate search attribute entries found for key {pair.key.name}"
                )

    def __len__(self) -> int:
        return len(self.search_attributes)

    def __getitem__(
        self, key: SearchAttributeKey[SearchAttributeValueType]
    ) -> SearchAttributeValueType:
        """Get a single search attribute value by key or fail with ``KeyError``."""
        ret = next((v for k, v in self if k == key), None)
        if ret is None:
            raise KeyError()
        return cast(SearchAttributeValueType, ret)

    def __iter__(self) -> Iterator[SearchAttributePair[Any]]:
        return iter(self.search_attributes)

    def __contains__(self, key: object) -> bool:
        """Check whether this collection contains the given key.

        This uses key equality so the key must be the same name and type.
        """
        return any(k == key for k, _v in self)

    @overload
    def get(
        self, key: SearchAttributeKey[SearchAttributeValueType]
    ) -> Optional[SearchAttributeValueType]: ...

    @overload
    def get(
        self,
        key: SearchAttributeKey[SearchAttributeValueType],
        default: _DefaultT,
    ) -> Union[SearchAttributeValueType, _DefaultT]: ...

    def get(
        self,
        key: SearchAttributeKey[SearchAttributeValueType],
        default: Optional[Any] = None,
    ) -> Any:
        """Get an attribute value for a key (or default), similar to dict.get."""
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def updated(
        self, *search_attributes: SearchAttributePair[Any]
    ) -> TypedSearchAttributes:
        """Copy this collection, replacing attributes with matching key names or
        adding if the key name is not present.
        """
        attrs = list(self.search_attributes)
        for attr in search_attributes:
            existing_index = next(
                (
                    i
                    for i, index_attr in enumerate(attrs)
                    if attr.key.name == index_attr.key.name
                ),
                None,
            )
            if existing_index is None:
                attrs.append(attr)
            else:
                attrs[existing_index] = attr
        return TypedSearchAttributes(attrs)


TypedSearchAttributes.empty = TypedSearchAttributes(search_attributes=[])


def _warn_on_deprecated_search_attributes(
    attributes: Optional[Union[SearchAttributes, Any]],
    stack_level: int = 2,
) -> None:
    if attributes and isinstance(attributes, Mapping):
        warnings.warn(
            "Dictionary-based search attributes are deprecated",
            DeprecationWarning,
            stacklevel=stack_level + 1,
        )


# ---------------------------------------------------------------------------
# Worker versioning / deployments
# ---------------------------------------------------------------------------
#
# Temporal's Worker Deployment Versioning lets a workflow be pinned to (or
# auto-upgraded across) worker build versions for safe rolling deploys. In
# temporal-dbos a "deployment version" is derived from DBOS's own versioning:
# ``deployment_name`` is the DBOS application name and ``build_id`` is the DBOS
# ``application_version`` (which already scopes recovery and queue dequeuing).
# These types mirror ``temporalio.common`` for signature parity; the behavior
# they request (pin vs. auto-upgrade routing) is inert — DBOS pins dequeue to
# ``application_version`` regardless — so they are accepted and surfaced (e.g.
# ``workflow.Info.get_current_deployment_version()``) but do not change
# scheduling (see DEVIATIONS D29).


class VersioningBehavior(IntEnum):
    """Specifies when a workflow might move from a worker of one Build Id to
    another, mirroring ``temporalio.common.VersioningBehavior``.

    Accepted for parity; inert in temporal-dbos (DBOS pins dequeue to
    ``application_version`` — see DEVIATIONS D29).
    """

    UNSPECIFIED = 0
    """An unspecified versioning behavior."""
    PINNED = 1
    """The workflow will be pinned to the current Build ID unless manually moved."""
    AUTO_UPGRADE = 2
    """The workflow will automatically move to the latest version (default Build
    ID of the task queue) when the next task is dispatched."""


@dataclass(frozen=True)
class WorkerDeploymentVersion:
    """Represents the version of a specific worker deployment, mirroring
    ``temporalio.common.WorkerDeploymentVersion``.

    In temporal-dbos ``deployment_name`` is the DBOS application name and
    ``build_id`` is the DBOS ``application_version``.
    """

    deployment_name: str
    build_id: str

    def to_canonical_string(self) -> str:
        """Returns the canonical string representation of the version."""
        return f"{self.deployment_name}.{self.build_id}"

    @staticmethod
    def from_canonical_string(canonical: str) -> "WorkerDeploymentVersion":
        """Parse a version from a canonical string, which must be in the format
        ``<deployment_name>.<build_id>``. Deployment name must not have a ``.``
        in it.
        """
        parts = canonical.split(".", maxsplit=1)
        if len(parts) != 2:
            raise ValueError(
                f"Cannot parse version string: {canonical}, must be in format "
                "<deployment_name>.<build_id>"
            )
        return WorkerDeploymentVersion(parts[0], parts[1])


class VersioningOverride(ABC):
    """Represents the override of a worker's versioning behavior for a workflow
    execution, mirroring ``temporalio.common.VersioningOverride``.

    Accepted for parity; inert in temporal-dbos (DEVIATIONS D29).
    """


@dataclass(frozen=True)
class PinnedVersioningOverride(VersioningOverride):
    """Workflow will be pinned to a specific deployment version."""

    version: WorkerDeploymentVersion


@dataclass(frozen=True)
class AutoUpgradeVersioningOverride(VersioningOverride):
    """The workflow will auto-upgrade to the current deployment version on the
    next workflow task."""
