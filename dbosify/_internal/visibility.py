"""Visibility-query parsing for ``client.list_workflows`` / ``count_workflows``.

Temporal sends the visibility filter *string* to the server, which parses it;
the ``temporalio`` SDK ships no parser (``list_workflows`` passes the string
straight into the gRPC request). DBOS, on the other side, takes structured
keyword filters, not a query string. So we hand-roll a small parser for the
documented subset and translate it into DBOS ``list_workflows`` filters, reusing
our own status mapping (:mod:`dbosify._internal.status`) and the stored
search-attribute shape (:mod:`dbosify._internal.attributes`).

Supported grammar — a flat ``AND`` conjunction, case-insensitive keywords::

    query    := clause (AND clause)*
    clause   := field operator value
    field    := WorkflowType | WorkflowId | ExecutionStatus | StartTime
              | CloseTime | <custom search-attribute name>
    operator := = | != | > | >= | < | <= | IN | STARTS_WITH
    value    := 'string' | number | true | false | ( value , value , ... )

The value tuple form is only valid after ``IN``. Anything outside this subset
(``OR``, grouping parentheses, ``ORDER BY`` / ``GROUP BY``, unsupported
field/operator pairings) is rejected with a message listing what *is* supported.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, cast

from dbos import WorkflowStatus

from .ids import RUN_SEPARATOR
from .status import WorkflowExecutionStatus, to_execution_status

# Reserved (system) field names the parser understands; everything else is
# treated as a custom search attribute. Matched case-insensitively.
_SYSTEM_FIELDS = {
    "workflowtype": "WorkflowType",
    "workflowid": "WorkflowId",
    "executionstatus": "ExecutionStatus",
    "starttime": "StartTime",
    "closetime": "CloseTime",
}

# Temporal ExecutionStatus name -> our enum. Accepts Temporal's PascalCase and our
# SCREAMING_SNAKE names, plus the British ``Cancelled`` for forgiveness.
_STATUS_BY_NAME: Dict[str, WorkflowExecutionStatus] = {
    "running": WorkflowExecutionStatus.RUNNING,
    "completed": WorkflowExecutionStatus.COMPLETED,
    "failed": WorkflowExecutionStatus.FAILED,
    "canceled": WorkflowExecutionStatus.CANCELED,
    "cancelled": WorkflowExecutionStatus.CANCELED,
    "terminated": WorkflowExecutionStatus.TERMINATED,
    "continuedasnew": WorkflowExecutionStatus.CONTINUED_AS_NEW,
    "continued_as_new": WorkflowExecutionStatus.CONTINUED_AS_NEW,
    "timedout": WorkflowExecutionStatus.TIMED_OUT,
    "timed_out": WorkflowExecutionStatus.TIMED_OUT,
}

# Each Temporal status -> the DBOS status string(s) that can hold it. The four
# ERROR-family statuses all collapse onto DBOS ``ERROR`` and need a post-filter.
_TEMPORAL_TO_DBOS: Dict[WorkflowExecutionStatus, Tuple[str, ...]] = {
    WorkflowExecutionStatus.RUNNING: ("PENDING", "ENQUEUED", "DELAYED"),
    WorkflowExecutionStatus.COMPLETED: ("SUCCESS",),
    WorkflowExecutionStatus.TERMINATED: ("CANCELLED",),
    WorkflowExecutionStatus.FAILED: ("ERROR",),
    WorkflowExecutionStatus.CANCELED: ("ERROR",),
    WorkflowExecutionStatus.TIMED_OUT: ("ERROR",),
    WorkflowExecutionStatus.CONTINUED_AS_NEW: ("ERROR",),
}

# Canonical Temporal status name (the spelling that appears in a count's
# group_values), keyed by our enum.
TEMPORAL_STATUS_NAME: Dict[WorkflowExecutionStatus, str] = {
    WorkflowExecutionStatus.RUNNING: "Running",
    WorkflowExecutionStatus.COMPLETED: "Completed",
    WorkflowExecutionStatus.FAILED: "Failed",
    WorkflowExecutionStatus.CANCELED: "Canceled",
    WorkflowExecutionStatus.TERMINATED: "Terminated",
    WorkflowExecutionStatus.CONTINUED_AS_NEW: "ContinuedAsNew",
    WorkflowExecutionStatus.TIMED_OUT: "TimedOut",
}

_SUPPORTED = (
    "Supported visibility subset: WorkflowType (= != IN), "
    "WorkflowId (= STARTS_WITH), ExecutionStatus (= IN), "
    "StartTime/CloseTime (> >= < <= =), <SearchAttribute> (=), "
    "joined by AND, with an optional trailing "
    "GROUP BY ExecutionStatus|WorkflowType (count_workflows only)."
)


class VisibilityQueryError(ValueError):
    """The visibility query used a construct outside the supported subset."""


# --- tokenizer ----------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""
      (?P<ws>\s+)
    | (?P<str>'(?:[^']|'')*')
    | (?P<dqstr>"(?:[^"]|"")*")
    | (?P<op><=|>=|!=|=|<|>)
    | (?P<lparen>\()
    | (?P<rparen>\))
    | (?P<comma>,)
    | (?P<num>-?\d+(?:\.\d+)?)
    | (?P<ident>[A-Za-z_][A-Za-z0-9_.]*)
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class _Token:
    kind: str  # "op" | "lparen" | "rparen" | "comma" | "num" | "ident" | "str"
    value: Any  # str for ident/op; parsed scalar for num/str


def _tokenize(query: str) -> List[_Token]:
    tokens: List[_Token] = []
    pos = 0
    while pos < len(query):
        m = _TOKEN_RE.match(query, pos)
        if m is None:
            raise VisibilityQueryError(
                f"Cannot parse visibility query near {query[pos:][:20]!r}. "
                + _SUPPORTED
            )
        pos = m.end()
        kind = m.lastgroup
        assert kind is not None
        if kind == "ws":
            continue
        text = m.group()
        if kind == "str":
            tokens.append(_Token("str", text[1:-1].replace("''", "'")))
        elif kind == "dqstr":
            tokens.append(_Token("str", text[1:-1].replace('""', '"')))
        elif kind == "num":
            tokens.append(_Token("num", float(text) if "." in text else int(text)))
        else:
            tokens.append(_Token(kind, text))
    return tokens


# --- parsed query -------------------------------------------------------------


@dataclass
class VisibilityQuery:
    """A parsed visibility filter, in terms of our own filter concepts.

    ``None`` fields mean "unconstrained". Fields that DBOS can enforce natively
    become ``list_workflows`` kwargs (:meth:`to_dbos_filters`); the rest are
    applied row-by-row (:meth:`post_filter`).
    """

    # WorkflowType: equality/IN go to DBOS ``name`` (as ``wf:{type}``); ``!=``
    # has no native DBOS form, so it post-filters.
    type_in: Optional[List[str]] = None
    type_not_in: List[str] = field(default_factory=list)
    workflow_ids: Optional[List[str]] = None
    workflow_id_prefix: Optional[str] = None
    # ``WorkflowId = X``: matches the whole run chain (X and its successors X--r{n}),
    # as in Temporal. A DBOS prefix query plus a post-filter narrows to the chain.
    workflow_id_chain: Optional[str] = None
    statuses: Optional[Set[WorkflowExecutionStatus]] = None
    start_time_lo: Optional[datetime] = None
    start_time_hi: Optional[datetime] = None
    close_time_lo: Optional[datetime] = None
    close_time_hi: Optional[datetime] = None
    # custom search attributes: name -> JSON scalar for ``@>`` containment.
    search_attributes: Dict[str, Any] = field(default_factory=dict)
    # Trailing ``GROUP BY`` (count_workflows only): canonical field name
    # ("ExecutionStatus" or "WorkflowType"), or None.
    group_by: Optional[str] = None

    def to_dbos_filters(self) -> Dict[str, Any]:
        """Kwargs for ``DBOSClient.list_workflows_async`` enforcing everything
        DBOS can do natively. The remainder is handled by :meth:`post_filter`."""
        filters: Dict[str, Any] = {}
        if self.type_in is not None:
            filters["name"] = [f"wf:{t}" for t in self.type_in]
        if self.workflow_ids is not None:
            filters["workflow_ids"] = self.workflow_ids
        if self.workflow_id_prefix is not None:
            # A list (not a bare str) so it's uniform across both DBOS calls:
            # get_workflow_aggregates iterates it, so a bare str matches per-char.
            filters["workflow_id_prefix"] = [self.workflow_id_prefix]
        if self.workflow_id_chain is not None:
            # The chain match runs as a DBOS prefix query (base id prefixes every
            # X--r{n}); post_filter() narrows those rows back to the chain.
            filters.setdefault("workflow_id_prefix", [self.workflow_id_chain])
        if self.statuses is not None:
            dbos_statuses: List[str] = []
            for s in self.statuses:
                for ds in _TEMPORAL_TO_DBOS[s]:
                    if ds not in dbos_statuses:
                        dbos_statuses.append(ds)
            filters["status"] = dbos_statuses
        if self.start_time_lo is not None:
            filters["start_time"] = self.start_time_lo.isoformat()
        if self.start_time_hi is not None:
            filters["end_time"] = self.start_time_hi.isoformat()
        if self.close_time_lo is not None:
            filters["completed_after"] = self.close_time_lo.isoformat()
        if self.close_time_hi is not None:
            filters["completed_before"] = self.close_time_hi.isoformat()
        if self.search_attributes:
            filters["attributes"] = {
                "search_attributes": {
                    name: {"v": value} for name, value in self.search_attributes.items()
                }
            }
        return filters

    def post_filter(self) -> Optional[Callable[[WorkflowStatus], bool]]:
        """A row predicate for what DBOS can't express: distinguishing the four
        ERROR-family ``ExecutionStatus`` values (all DBOS ``ERROR``) and
        ``WorkflowType !=``. ``None`` when no post-filtering is needed."""
        needs_status = self.statuses is not None and any(
            _TEMPORAL_TO_DBOS[s] == ("ERROR",) for s in self.statuses
        )
        chain = self.workflow_id_chain
        if not needs_status and not self.type_not_in and chain is None:
            return None

        statuses = self.statuses
        not_in = set(self.type_not_in)
        chain_prefix = (chain + RUN_SEPARATOR) if chain is not None else None

        def predicate(row: WorkflowStatus) -> bool:
            if chain is not None:
                # Keep only the chain: exact base id or a run-chain successor
                # X--r{n} (RUN_SEPARATOR keeps unrelated X-prefixed ids distinct).
                assert chain_prefix is not None
                if not (
                    row.workflow_id == chain or row.workflow_id.startswith(chain_prefix)
                ):
                    return False
            if statuses is not None:
                actual = to_execution_status(row.status, error=row.error)
                if actual not in statuses:
                    return False
            if not_in:
                type_name = row.name or ""
                if type_name.startswith("wf:"):
                    type_name = type_name[3:]
                if type_name in not_in:
                    return False
            return True

        return predicate

    def aggregate_eligible(self) -> bool:
        """Whether this query's *filters* can be expressed by the server-side
        ``get_workflow_aggregates`` operator. It has no exact-id, no JSONB
        search-attribute, and no name-``!=`` predicate, so those force a scan.
        (An ERROR-family ``ExecutionStatus`` restriction also needs the marker,
        i.e. a non-None :meth:`post_filter`; callers check that separately.)"""
        return (
            not self.search_attributes
            and self.workflow_ids is None
            and self.workflow_id_chain is None
            and not self.type_not_in
        )

    def aggregate_filter_kwargs(self) -> Dict[str, Any]:
        """The subset of :meth:`to_dbos_filters` that ``get_workflow_aggregates``
        accepts as filters (``workflow_ids`` and ``attributes`` are not
        expressible there, so eligibility excludes queries that use them)."""
        f = self.to_dbos_filters()
        return {
            key: f[key]
            for key in (
                "name",
                "status",
                "start_time",
                "end_time",
                "completed_after",
                "completed_before",
                "workflow_id_prefix",
            )
            if key in f
        }


# --- parser -------------------------------------------------------------------


def _parse_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise VisibilityQueryError(
            f"{field_name} expects a quoted ISO-8601 datetime, got {value!r}"
        )
    dt: datetime
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        try:
            from dateutil import parser as _dateutil_parser

            # dateutil ships no stubs, so isoparse is Any; cast for mypy --strict.
            dt = cast(datetime, _dateutil_parser.isoparse(value))
        except (ValueError, ImportError):
            raise VisibilityQueryError(
                f"{field_name}: cannot parse {value!r} as an ISO-8601 datetime"
            ) from None
    return dt


def _apply_clause(q: VisibilityQuery, field_name: str, op: str, value: Any) -> None:
    canonical = _SYSTEM_FIELDS.get(field_name.lower())

    if canonical == "WorkflowType":
        values = value if isinstance(value, list) else [value]
        names = [str(v) for v in values]
        if op == "=" or op == "IN":
            # Repeated positive WorkflowType clauses are joined by AND, so they
            # intersect (mirrors ExecutionStatus); a first clause just sets it.
            if q.type_in is None:
                q.type_in = list(names)
            else:
                allowed = set(names)
                q.type_in = [t for t in q.type_in if t in allowed]
        elif op == "!=":
            q.type_not_in.extend(names)
        else:
            raise VisibilityQueryError(
                f"WorkflowType supports = != IN, not {op!r}. " + _SUPPORTED
            )
        return

    if canonical == "WorkflowId":
        if isinstance(value, list):
            raise VisibilityQueryError("WorkflowId does not support a value list")
        if op == "=":
            # All runs of this workflow id (the run chain), as in Temporal.
            q.workflow_id_chain = str(value)
        elif op == "STARTS_WITH":
            q.workflow_id_prefix = str(value)
        else:
            raise VisibilityQueryError(
                f"WorkflowId supports = and STARTS_WITH, not {op!r}. " + _SUPPORTED
            )
        return

    if canonical == "ExecutionStatus":
        if op not in ("=", "IN"):
            raise VisibilityQueryError(
                f"ExecutionStatus supports = and IN, not {op!r}. " + _SUPPORTED
            )
        values = value if isinstance(value, list) else [value]
        parsed: Set[WorkflowExecutionStatus] = set()
        for v in values:
            key = str(v).lower()
            status = _STATUS_BY_NAME.get(key)
            if status is None:
                raise VisibilityQueryError(
                    f"Unknown ExecutionStatus {v!r}. Valid: Running, Completed, "
                    "Failed, Canceled, Terminated, ContinuedAsNew, TimedOut."
                )
            parsed.add(status)
        # Multiple ExecutionStatus clauses intersect (joined by AND).
        q.statuses = parsed if q.statuses is None else (q.statuses & parsed)
        return

    if canonical in ("StartTime", "CloseTime"):
        if isinstance(value, list):
            raise VisibilityQueryError(f"{canonical} does not support a value list")
        dt = _parse_datetime(value, canonical)
        lo_attr = "start_time_lo" if canonical == "StartTime" else "close_time_lo"
        hi_attr = "start_time_hi" if canonical == "StartTime" else "close_time_hi"

        def tighten_lo() -> None:
            cur = getattr(q, lo_attr)
            setattr(q, lo_attr, dt if cur is None else max(cur, dt))

        def tighten_hi() -> None:
            cur = getattr(q, hi_attr)
            setattr(q, hi_attr, dt if cur is None else min(cur, dt))

        if op in (">", ">="):
            tighten_lo()
        elif op in ("<", "<="):
            tighten_hi()
        elif op == "=":
            tighten_lo()
            tighten_hi()
        else:
            raise VisibilityQueryError(
                f"{canonical} supports > >= < <= =, not {op!r}. " + _SUPPORTED
            )
        return

    # Anything else is a custom search attribute (equality only in v1).
    if op != "=":
        raise VisibilityQueryError(
            f"Search attribute {field_name!r} supports only = (got {op!r}). "
            + _SUPPORTED
        )
    if isinstance(value, list):
        raise VisibilityQueryError(
            f"Search attribute {field_name!r} does not support a value list"
        )
    q.search_attributes[field_name] = value


def parse_query(query: Optional[str]) -> VisibilityQuery:
    """Parse a visibility filter string into a :class:`VisibilityQuery`. An
    empty/``None`` query matches everything."""
    q = VisibilityQuery()
    if query is None or not query.strip():
        return q

    tokens = _tokenize(query)
    i = 0
    n = len(tokens)

    def fail(msg: str) -> "VisibilityQueryError":
        return VisibilityQueryError(msg + " " + _SUPPORTED)

    while i < n:
        tok = tokens[i]
        # Trailing GROUP BY <field> (count_workflows only) — terminal.
        if tok.kind == "ident" and str(tok.value).upper() == "GROUP":
            i += 1
            if (
                i >= n
                or tokens[i].kind != "ident"
                or str(tokens[i].value).upper() != "BY"
            ):
                raise fail("Expected 'BY' after GROUP.")
            i += 1
            if i >= n or tokens[i].kind != "ident":
                raise fail("GROUP BY requires a field name.")
            gb_canon = _SYSTEM_FIELDS.get(str(tokens[i].value).lower())
            if gb_canon not in ("ExecutionStatus", "WorkflowType"):
                raise fail(
                    "GROUP BY supports only ExecutionStatus or WorkflowType, got "
                    f"{tokens[i].value!r}."
                )
            i += 1
            q.group_by = gb_canon
            if i != n:
                raise fail("GROUP BY must be the final clause.")
            break

        # field
        if tok.kind != "ident":
            raise fail(f"Expected a field name, got {tok.value!r}.")
        upper = str(tok.value).upper()
        if upper in ("AND", "OR", "ORDER", "GROUP", "IN", "STARTS_WITH"):
            raise fail(f"Unexpected keyword {tok.value!r} where a field was expected.")
        field_name = str(tok.value)
        i += 1

        # operator
        if i >= n:
            raise fail(f"Expected an operator after {field_name!r}.")
        op_tok = tokens[i]
        if op_tok.kind == "op":
            op = str(op_tok.value)
            i += 1
        elif op_tok.kind == "ident" and str(op_tok.value).upper() == "IN":
            op = "IN"
            i += 1
        elif op_tok.kind == "ident" and str(op_tok.value).upper() == "STARTS_WITH":
            op = "STARTS_WITH"
            i += 1
        else:
            raise fail(
                f"Expected an operator after {field_name!r}, got {op_tok.value!r}."
            )

        # value (scalar, or parenthesized list for IN)
        if i >= n:
            raise fail(f"Expected a value after {field_name!r} {op}.")
        val_tok = tokens[i]
        if op == "IN":
            if val_tok.kind != "lparen":
                raise fail("IN requires a parenthesized list, e.g. IN ('a', 'b').")
            i += 1
            items: List[Any] = []
            while i < n and tokens[i].kind != "rparen":
                item = tokens[i]
                if item.kind not in ("str", "num"):
                    raise fail(f"IN list takes literals, got {item.value!r}.")
                items.append(_scalar(item))
                i += 1
                if i < n and tokens[i].kind == "comma":
                    i += 1
                elif i < n and tokens[i].kind != "rparen":
                    raise fail("Expected ',' or ')' in IN list.")
            if i >= n:
                raise fail("Unterminated IN list (missing ')').")
            i += 1  # consume rparen
            value: Any = items
        else:
            if val_tok.kind == "lparen":
                raise fail("Parenthesized values are only valid after IN.")
            if val_tok.kind == "ident":
                value = _ident_literal(str(val_tok.value))
            elif val_tok.kind in ("str", "num"):
                value = _scalar(val_tok)
            else:
                raise fail(f"Expected a value, got {val_tok.value!r}.")
            i += 1

        _apply_clause(q, field_name, op, value)

        # conjunction: either end, or AND <next clause>
        if i < n:
            conj = tokens[i]
            if conj.kind == "ident" and str(conj.value).upper() == "AND":
                i += 1
                if i >= n:
                    raise fail("Trailing AND with no clause.")
                continue
            if conj.kind == "ident" and str(conj.value).upper() == "GROUP":
                continue  # GROUP BY is handled at the loop top
            if conj.kind == "ident" and str(conj.value).upper() == "OR":
                raise fail("OR is not supported; only AND.")
            raise fail(f"Expected AND or end of query, got {conj.value!r}.")

    return q


def _scalar(token: _Token) -> Any:
    if token.kind == "num":
        return token.value
    return token.value  # str


def _ident_literal(text: str) -> Any:
    """A bareword value: booleans, otherwise the literal text (Temporal allows
    unquoted keywords like ``true``)."""
    low = text.lower()
    if low == "true":
        return True
    if low == "false":
        return False
    return text
