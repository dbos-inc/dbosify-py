"""Unit tests for the visibility-query parser (no Postgres):
field/operator coverage, AND conjunction, the SA-equality containment shape,
the ExecutionStatus mapping (incl. the ERROR-family post-filter), and rejection
of everything outside the supported grammar."""

import pytest
from dbos import WorkflowStatus

from dbosify._internal.payloads import (
    SerializedContinueAsNew,
    SerializedWorkflowCancellation,
)
from dbosify._internal.status import WorkflowExecutionStatus
from dbosify._internal.visibility import (
    VisibilityQueryError,
    parse_query,
)


def _row(
    status: str = "SUCCESS",
    *,
    name: str = "wf:Foo",
    error: object = None,
    workflow_id: str = "",
) -> WorkflowStatus:
    """A bare DBOS WorkflowStatus carrying just the fields post_filter reads."""
    s = WorkflowStatus()
    s.status = status  # type: ignore[assignment]
    s.name = name
    s.error = error  # type: ignore[assignment]
    s.workflow_id = workflow_id
    return s


# --- empty / passthrough ------------------------------------------------------


@pytest.mark.parametrize("q", [None, "", "   "])
def test_empty_query_matches_everything(q: object) -> None:
    parsed = parse_query(q)  # type: ignore[arg-type]
    assert parsed.to_dbos_filters() == {}
    assert parsed.post_filter() is None


# --- WorkflowType -------------------------------------------------------------


def test_workflow_type_eq_maps_to_prefixed_name() -> None:
    parsed = parse_query("WorkflowType = 'Greeter'")
    assert parsed.to_dbos_filters() == {"name": ["wf:Greeter"]}
    assert parsed.post_filter() is None


def test_workflow_type_in_maps_to_name_list() -> None:
    parsed = parse_query("WorkflowType IN ('A', 'B')")
    assert parsed.to_dbos_filters() == {"name": ["wf:A", "wf:B"]}


def test_workflow_type_repeated_equality_intersects() -> None:
    # Two positive WorkflowType clauses joined by AND intersect (like
    # ExecutionStatus) — not union. A=B is impossible → matches nothing.
    parsed = parse_query("WorkflowType = 'A' AND WorkflowType = 'B'")
    assert parsed.type_in == []
    assert parsed.to_dbos_filters()["name"] == []


def test_workflow_type_eq_then_in_intersects() -> None:
    parsed = parse_query("WorkflowType = 'A' AND WorkflowType IN ('A', 'B')")
    assert parsed.type_in == ["A"]


def test_time_bound_repeated_clause_tightens() -> None:
    # AND of two lower bounds keeps the tighter (later) one, not the last seen.
    parsed = parse_query(
        "StartTime >= '2024-06-01T00:00:00+00:00' "
        "AND StartTime >= '2024-01-01T00:00:00+00:00'"
    )
    assert parsed.start_time_lo is not None
    assert parsed.start_time_lo.month == 6


def test_workflow_type_ne_post_filters() -> None:
    parsed = parse_query("WorkflowType != 'Skip'")
    # No native DBOS form: nothing in the kwargs, a predicate instead.
    assert "name" not in parsed.to_dbos_filters()
    pf = parsed.post_filter()
    assert pf is not None
    assert pf(_row("SUCCESS", name="wf:Keep")) is True
    assert pf(_row("SUCCESS", name="wf:Skip")) is False


# --- WorkflowId ---------------------------------------------------------------


def test_workflow_id_eq() -> None:
    # WorkflowId = X matches the whole run chain (X and its successors X--r{n}),
    # as in Temporal: a DBOS prefix query narrowed by a post_filter to chain runs.
    parsed = parse_query("WorkflowId = 'order-1'")
    assert parsed.to_dbos_filters() == {"workflow_id_prefix": ["order-1"]}
    assert not parsed.aggregate_eligible()  # an id filter forces a scan
    pred = parsed.post_filter()
    assert pred is not None
    assert pred(_row(workflow_id="order-1"))  # base run
    assert pred(_row(workflow_id="order-1--r2"))  # a run-chain successor
    assert not pred(_row(workflow_id="order-1x"))  # unrelated id sharing prefix
    assert not pred(_row(workflow_id="order-2"))


def test_workflow_id_starts_with() -> None:
    # A list (not a bare str) so it's uniform across both DBOS calls.
    assert parse_query("WorkflowId STARTS_WITH 'order-'").to_dbos_filters() == {
        "workflow_id_prefix": ["order-"]
    }


def test_workflow_id_bad_operator_rejected() -> None:
    with pytest.raises(VisibilityQueryError):
        parse_query("WorkflowId > 'x'")


# --- ExecutionStatus ----------------------------------------------------------


def test_status_running_is_clean_multi_map() -> None:
    parsed = parse_query("ExecutionStatus = 'Running'")
    # MAX_RECOVERY_ATTEMPTS_EXCEEDED is included: a stuck workflow maps to RUNNING
    # in describe(), so the filter must surface it too.
    assert parsed.to_dbos_filters() == {
        "status": ["PENDING", "ENQUEUED", "DELAYED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED"]
    }
    assert parsed.post_filter() is None  # no ERROR-family member


def test_status_completed_and_terminated_clean() -> None:
    assert parse_query("ExecutionStatus = 'Completed'").to_dbos_filters() == {
        "status": ["SUCCESS"]
    }
    assert parse_query("ExecutionStatus = 'Terminated'").to_dbos_filters() == {
        "status": ["CANCELLED"]
    }


def test_status_failed_uses_error_plus_post_filter() -> None:
    parsed = parse_query("ExecutionStatus = 'Failed'")
    assert parsed.to_dbos_filters() == {"status": ["ERROR"]}
    pf = parsed.post_filter()
    assert pf is not None
    # ERROR + plain failure -> FAILED (kept); ERROR + cancellation marker ->
    # CANCELED (dropped); the cancel/CAN markers share DBOS "ERROR".
    assert pf(_row("ERROR", error=ValueError("boom"))) is True
    assert pf(_row("ERROR", error=SerializedWorkflowCancellation({}))) is False
    assert (
        pf(_row("ERROR", error=SerializedContinueAsNew({"new_run_id": "x--r1"})))
        is False
    )


def test_status_canceled_post_filter_keeps_only_cancellations() -> None:
    parsed = parse_query("ExecutionStatus = 'Canceled'")
    assert parsed.to_dbos_filters() == {"status": ["ERROR"]}
    pf = parsed.post_filter()
    assert pf is not None
    assert pf(_row("ERROR", error=SerializedWorkflowCancellation({}))) is True
    assert pf(_row("ERROR", error=ValueError("boom"))) is False


def test_status_in_unions_dbos_statuses() -> None:
    parsed = parse_query("ExecutionStatus IN ('Running', 'Completed')")
    statuses = parsed.to_dbos_filters()["status"]
    assert set(statuses) == {
        "PENDING",
        "ENQUEUED",
        "DELAYED",
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        "SUCCESS",
    }
    assert parsed.post_filter() is None


def test_status_alternate_spellings() -> None:
    # British spelling + our SCREAMING_SNAKE enum names are accepted.
    assert parse_query("ExecutionStatus = 'Cancelled'").to_dbos_filters()["status"] == [
        "ERROR"
    ]
    assert parse_query("ExecutionStatus = 'CONTINUED_AS_NEW'").to_dbos_filters()[
        "status"
    ] == ["ERROR"]


def test_status_intersection_when_repeated() -> None:
    # AND of two equalities intersects to empty -> matches nothing.
    parsed = parse_query("ExecutionStatus = 'Running' AND ExecutionStatus = 'Failed'")
    assert parsed.statuses == set()
    assert parsed.to_dbos_filters()["status"] == []


def test_unknown_status_rejected() -> None:
    with pytest.raises(VisibilityQueryError):
        parse_query("ExecutionStatus = 'Bogus'")


# --- time ranges --------------------------------------------------------------


def test_start_time_range() -> None:
    f = parse_query(
        "StartTime >= '2024-01-01T00:00:00+00:00' "
        "AND StartTime < '2024-02-01T00:00:00+00:00'"
    ).to_dbos_filters()
    assert f["start_time"].startswith("2024-01-01T00:00:00")
    assert f["end_time"].startswith("2024-02-01T00:00:00")


def test_close_time_maps_to_completed_bounds() -> None:
    f = parse_query("CloseTime > '2024-01-01T00:00:00+00:00'").to_dbos_filters()
    assert "completed_after" in f and "completed_before" not in f


def test_bad_datetime_rejected() -> None:
    with pytest.raises(VisibilityQueryError):
        parse_query("StartTime > 'not-a-date'")


# --- search attributes --------------------------------------------------------


def test_search_attribute_string_containment_shape() -> None:
    f = parse_query("CustomKeywordField = 'foo'").to_dbos_filters()
    assert f == {
        "attributes": {"search_attributes": {"CustomKeywordField": {"v": "foo"}}}
    }


def test_search_attribute_int_and_bool() -> None:
    f = parse_query("Count = 5 AND Flag = true").to_dbos_filters()
    sa = f["attributes"]["search_attributes"]
    assert sa["Count"] == {"v": 5}
    assert sa["Flag"] == {"v": True}


def test_search_attribute_only_equality() -> None:
    with pytest.raises(VisibilityQueryError):
        parse_query("CustomKeywordField > 'foo'")


# --- conjunction + literals ---------------------------------------------------


def test_and_combines_heterogeneous_clauses() -> None:
    f = parse_query(
        "WorkflowType = 'Greeter' AND ExecutionStatus = 'Completed' "
        "AND CustomKeywordField = 'vip'"
    ).to_dbos_filters()
    assert f["name"] == ["wf:Greeter"]
    assert f["status"] == ["SUCCESS"]
    assert f["attributes"] == {
        "search_attributes": {"CustomKeywordField": {"v": "vip"}}
    }


def test_escaped_quote_in_string_literal() -> None:
    parsed = parse_query("WorkflowId = 'O''Brien'")
    assert parsed.to_dbos_filters()["workflow_id_prefix"] == ["O'Brien"]


def test_and_inside_quoted_value_is_not_a_conjunction() -> None:
    # The tokenizer must not split on AND inside a string.
    parsed = parse_query("WorkflowId = 'a AND b'")
    assert parsed.to_dbos_filters()["workflow_id_prefix"] == ["a AND b"]


# --- rejections of unsupported constructs -------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "WorkflowType = 'A' OR WorkflowType = 'B'",  # OR
        "(WorkflowType = 'A')",  # grouping parens
        "WorkflowType = 'A' ORDER BY StartTime",  # ORDER BY
        "WorkflowType IN 'A'",  # IN without parens
        "WorkflowType IN ('A'",  # unterminated IN list
        "WorkflowType = 'A' AND",  # trailing AND
        "WorkflowType",  # missing operator + value
        "= 'A'",  # missing field
        "WorkflowType == 'A'",  # not a real operator (== tokenizes as two =)
    ],
)
def test_unsupported_constructs_rejected(query: str) -> None:
    with pytest.raises(VisibilityQueryError):
        parse_query(query)


# --- GROUP BY + aggregate eligibility -----------------------------------------


def test_group_by_execution_status() -> None:
    q = parse_query("GROUP BY ExecutionStatus")
    assert q.group_by == "ExecutionStatus"


def test_group_by_workflow_type_with_filters() -> None:
    q = parse_query("ExecutionStatus = 'Completed' GROUP BY WorkflowType")
    assert q.group_by == "WorkflowType"
    assert q.to_dbos_filters()["status"] == ["SUCCESS"]


@pytest.mark.parametrize(
    "query",
    [
        "GROUP ExecutionStatus",  # missing BY
        "GROUP BY",  # missing field
        "GROUP BY StartTime",  # not a groupable field
        "GROUP BY CustomKeyword",  # search attribute not groupable
        "GROUP BY ExecutionStatus AND WorkflowType = 'X'",  # not final
        "GROUP BY ExecutionStatus GROUP BY WorkflowType",  # two group-bys
    ],
)
def test_bad_group_by_rejected(query: str) -> None:
    with pytest.raises(VisibilityQueryError):
        parse_query(query)


def test_aggregate_eligible_for_plain_filters() -> None:
    assert parse_query("WorkflowType = 'A'").aggregate_eligible() is True
    assert parse_query("ExecutionStatus = 'Completed'").aggregate_eligible() is True
    assert parse_query("WorkflowId STARTS_WITH 'o'").aggregate_eligible() is True


def test_aggregate_not_eligible_for_sa_id_or_negation() -> None:
    assert parse_query("CustomKeyword = 'x'").aggregate_eligible() is False
    assert parse_query("WorkflowId = 'exact'").aggregate_eligible() is False
    assert parse_query("WorkflowType != 'X'").aggregate_eligible() is False


def test_aggregate_filter_kwargs_shape() -> None:
    kwargs = parse_query(
        "WorkflowType = 'A' AND ExecutionStatus = 'Completed' "
        "AND WorkflowId STARTS_WITH 'o'"
    ).aggregate_filter_kwargs()
    assert kwargs["name"] == ["wf:A"]
    assert kwargs["status"] == ["SUCCESS"]
    # workflow_id_prefix is wrapped in a list for get_workflow_aggregates.
    assert kwargs["workflow_id_prefix"] == ["o"]
