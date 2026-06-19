"""In-process behavior tests for ``workflow.get_current_details`` /
``set_current_details`` (audit item 3).

Current details are in-memory workflow state, reconstructed deterministically
on replay. They are settable from the run method and from signal/update
handlers (everything that runs on the deterministic loop) and default to the
empty string. The crash-and-reconstruct proof lives in
``test_current_details_recovery.py``.
"""

import pytest

from dbosify import workflow
from dbosify._internal import dispatcher


@workflow.defn
class DetailsWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def advance(self) -> None:
        # Signal handlers run on the deterministic loop and may mutate details.
        workflow.set_current_details("advanced")

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> str:
        # Defaults to empty before the first set.
        assert workflow.get_current_details() == ""
        workflow.set_current_details("initial")
        await workflow.wait_condition(lambda: self.done)
        return workflow.get_current_details()


@pytest.mark.usefixtures("dbosify")
def test_details_set_in_run_returned() -> None:
    dispatcher.register_worker(workflows=[DetailsWorkflow])
    handle = dispatcher.start_workflow(DetailsWorkflow, [], workflow_id="details-run")
    dispatcher.signal_workflow("details-run", "finish")
    # Default-empty assertion held, run() set "initial", get reads it back.
    assert dispatcher.workflow_result(handle) == "initial"


@pytest.mark.usefixtures("dbosify")
def test_details_updated_by_signal_handler() -> None:
    dispatcher.register_worker(workflows=[DetailsWorkflow])
    handle = dispatcher.start_workflow(DetailsWorkflow, [], workflow_id="details-sig")
    dispatcher.signal_workflow("details-sig", "advance")
    dispatcher.signal_workflow("details-sig", "finish")
    assert dispatcher.workflow_result(handle) == "advanced"
