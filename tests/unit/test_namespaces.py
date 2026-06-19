"""Namespace -> DBOS schema mapping (DEVIATIONS D1)."""

import pytest

from dbosify._internal.namespaces import (
    DEFAULT_NAMESPACE,
    namespace_from_schema,
    namespace_schema,
)


def test_default_namespace_is_not_privileged() -> None:
    # No back-compat carve-out: "default" maps to its own schema like any other.
    assert namespace_schema(DEFAULT_NAMESPACE) == "temporal_default"


@pytest.mark.parametrize(
    "namespace, schema",
    [
        ("alt", "temporal_alt"),
        ("my_namespace", "temporal_my_namespace"),
        ("team_a", "temporal_team_a"),
        ("_internal", "temporal__internal"),
        ("ns123", "temporal_ns123"),
        ("a" * 54, "temporal_" + "a" * 54),  # max length once prefixed (63)
    ],
)
def test_valid_namespace_maps_to_prefixed_schema(namespace: str, schema: str) -> None:
    assert namespace_schema(namespace) == schema


@pytest.mark.parametrize(
    "namespace",
    [
        "",  # empty
        "UPPER",  # uppercase folds/quotes ambiguously
        "Mixed",
        "with-hyphen",  # not an identifier char
        "with.dot",
        "has space",
        "1leading",  # must start with a letter or underscore
        "a" * 55,  # too long once prefixed (>63)
    ],
)
def test_invalid_namespace_rejected(namespace: str) -> None:
    with pytest.raises(ValueError, match="cannot back a Postgres schema"):
        namespace_schema(namespace)


@pytest.mark.parametrize("namespace", ["default", "alt", "my_namespace", "_internal"])
def test_namespace_schema_round_trips(namespace: str) -> None:
    # The low-level Client recovers the namespace from the schema.
    assert namespace_from_schema(namespace_schema(namespace)) == namespace


@pytest.mark.parametrize(
    "schema",
    [
        None,  # SQLite / unset
        "dbos",  # DBOS's default schema — not a namespace
        "public",
        "temporal_",  # prefix only, empty namespace
        "temporal_With-Hyphen",  # invalid namespace suffix
        "myschema",
    ],
)
def test_non_namespace_schema_rejected(schema: object) -> None:
    with pytest.raises(ValueError, match="not a temporal namespace schema"):
        namespace_from_schema(schema)  # type: ignore[arg-type]
