"""Shared harness for the accepted-parameter audits.

The Worker, Client, and start-verb audits each classify every temporalio
parameter of one or more callables into named buckets — some plain sets
(honored, rejected, deviation), some ``{param: reason}`` dicts (inert, subsumed,
pending) — and run the same four cross-cutting checks: every param is
classified, buckets are disjoint, reason-dicts have non-empty reasons, and the
honored bucket only contains parameters we actually accept as named arguments.

The signature extractor and those checks live here so the three audit files
share one implementation; each file keeps its own bucket definitions and any
audit-specific tests (e.g. the Worker's reject-raises check, the Client's
TypeError check).
"""

import inspect
from typing import Any, Callable, Iterable, Mapping, Set, Union

# Positional / dispatch arguments that are never options to classify.
_SKIP = frozenset({"self", "workflow", "arg", "args", "activity"})

# A bucket is a set of names, or a {name: reason} mapping (reasons get checked).
Bucket = Union[Set[str], Mapping[str, str]]


def named_params(fn: Callable[..., Any], *, skip: Iterable[str] = ()) -> Set[str]:
    """The explicit (non ``*args``/``**kwargs``) parameter names of ``fn``,
    minus the positional/dispatch names and any extra ``skip``."""
    drop = _SKIP | set(skip)
    return {
        p.name
        for p in inspect.signature(fn).parameters.values()
        if p.name not in drop
        and p.kind
        not in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)
    }


def _names(bucket: Bucket) -> Set[str]:
    return set(bucket)


def assert_every_param_classified(
    name: str, theirs: Set[str], buckets: Mapping[str, Bucket]
) -> None:
    classified: Set[str] = set()
    for bucket in buckets.values():
        classified |= _names(bucket)
    unclassified = theirs - classified
    assert not unclassified, (
        f"{name}: temporalio params not classified: {sorted(unclassified)} "
        f"— add each to one of {sorted(buckets)}."
    )
    stale = classified - theirs
    assert not stale, f"{name}: audit classifies non-params: {sorted(stale)}"


def assert_buckets_disjoint(name: str, buckets: Mapping[str, Bucket]) -> None:
    items = list(buckets.items())
    for i, (na, a) in enumerate(items):
        for nb, b in items[i + 1 :]:
            overlap = _names(a) & _names(b)
            assert (
                not overlap
            ), f"{name}: params in both {na} and {nb}: {sorted(overlap)}"


def assert_reasons_present(name: str, buckets: Mapping[str, Bucket]) -> None:
    for bname, bucket in buckets.items():
        if isinstance(bucket, Mapping):
            for param, reason in bucket.items():
                assert reason.strip(), f"{name}: blank reason for {bname}.{param}"


def assert_honored_are_named(name: str, honored: Set[str], ours: Set[str]) -> None:
    missing = honored - ours
    assert not missing, f"{name}: honored but not a named argument: {sorted(missing)}"
