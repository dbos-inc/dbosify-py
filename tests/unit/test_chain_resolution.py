"""Chain resolution via exact-id probes (ids.resolve_latest_run): no
prefix scans (unindexed in DBOS, unsafe on the critical path). Verifies
correctness across chain shapes and that round trips stay logarithmic.
"""

import asyncio
from typing import Any, Dict, Sequence

import pytest

from dbosify._internal import ids


class FakeChain:
    """A chain of `length` runs of workflow W (indexes 0..length-1), with
    optional deleted (GC'd) indexes; counts lookup round trips."""

    def __init__(self, length: int, deleted: Sequence[int] = ()) -> None:
        self.existing = {
            ids.run_dbos_id("W", i) for i in range(length) if i not in set(deleted)
        }
        self.queries = 0

    async def lookup(self, dbos_ids: Sequence[str]) -> Dict[str, Any]:
        self.queries += 1
        return {d: f"status:{d}" for d in dbos_ids if d in self.existing}


def _resolve(chain: FakeChain) -> Any:
    return asyncio.run(ids.resolve_latest_run("W", chain.lookup))


@pytest.mark.parametrize("length", [1, 2, 5, 16, 17, 33, 100, 1000, 70000])
def test_resolves_correct_latest(length: int) -> None:
    chain = FakeChain(length)
    resolved = _resolve(chain)
    assert resolved is not None
    index, status = resolved
    assert index == length - 1
    assert status == f"status:{ids.run_dbos_id('W', length - 1)}"


def test_missing_chain() -> None:
    chain = FakeChain(0)
    assert _resolve(chain) is None
    assert chain.queries == 1


def test_short_chains_resolve_in_one_round_trip() -> None:
    # Lengths up to DENSE_PROBE_LIMIT: the max index and its (missing)
    # successor are both inside the dense probe range.
    for length in range(1, ids.DENSE_PROBE_LIMIT + 1):
        chain = FakeChain(length)
        resolved = _resolve(chain)
        assert resolved is not None and resolved[0] == length - 1
        assert chain.queries == 1, f"chain of {length} took {chain.queries} queries"


def test_long_chains_resolve_in_logarithmic_round_trips() -> None:
    for length, max_queries in [(100, 3), (10_000, 5), (500_000, 7)]:
        chain = FakeChain(length)
        resolved = _resolve(chain)
        assert resolved is not None and resolved[0] == length - 1
        assert (
            chain.queries <= max_queries
        ), f"chain of {length} took {chain.queries} queries"


def test_tolerates_garbage_collected_old_runs() -> None:
    # Operator deletion / retention GC removes OLD runs; the live frontier
    # anchors resolution.
    chain = FakeChain(10, deleted=[0, 1, 2, 3])
    resolved = _resolve(chain)
    assert resolved is not None and resolved[0] == 9


@pytest.mark.parametrize(
    "dbos_id,expected",
    [
        ("W", ("W", 0)),
        ("W--r3", ("W", 3)),
        # Auto child ids embed their parent RUN id: the suffix has an
        # underscore, which int() would happily parse ("2_5" -> 25); the
        # digit guard must treat these as standalone base ids.
        ("W--r2_5", ("W--r2_5", 0)),
        ("W--r2_5--r1", ("W--r2_5", 1)),
        ("W--r2_5_3", ("W--r2_5_3", 0)),
    ],
)
def test_parse_run(dbos_id: str, expected: "tuple[str, int]") -> None:
    assert ids.parse_run(dbos_id) == expected
