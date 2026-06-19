"""Workflow-id namespacing (ids.py): the ``--r`` (run-chain) and ``--a``
(cross-queue activity, §6.1.2) separators are reserved so internal ids can never
collide with a user/child id — a collision would make the idempotent
``SetWorkflowID`` enqueue silently re-attach to an unrelated workflow.
"""

import pytest

from dbosify._internal import ids


@pytest.mark.parametrize(
    "good_id",
    ["order", "order-1", "order_5", "a-b-c", "user@example.com", "W-a1"],
)
def test_validate_accepts_ordinary_ids(good_id: str) -> None:
    ids.validate_workflow_id(good_id)  # does not raise


@pytest.mark.parametrize(
    "bad_id",
    ["W--r1", "foo--report", "W--a1", "order--archive", "x--a", "x--r"],
)
def test_validate_rejects_reserved_separators(bad_id: str) -> None:
    with pytest.raises(ValueError):
        ids.validate_workflow_id(bad_id)


def test_activity_id_is_in_the_reserved_namespace() -> None:
    # The id an activity of run W (or a chained run) gets...
    activity_id = ids.activity_dbos_id("W", 1)
    assert activity_id == "W--a1"

    chained = ids.activity_dbos_id(ids.run_dbos_id("W", 2), 3)
    assert chained == "W--r2--a3"

    # ...is itself an id a user could never have chosen (reserved), so it cannot
    # collide with a user/child id.
    for activity_workflow_id in (activity_id, chained):
        with pytest.raises(ValueError):
            ids.validate_workflow_id(activity_workflow_id)

    # And it is not mis-read as a run of W (no spurious chain attachment).
    assert ids.parse_run("W--a1") == ("W--a1", 0)
    assert ids.run_index_of("W", "W--a1") is None
