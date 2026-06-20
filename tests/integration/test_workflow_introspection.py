"""Workflow introspection helpers added to mirror temporalio: instance(),
current_update_info()/UpdateInfo, random_seed()/new_random(), and
ReadOnlyContextError. (info().root needs real children — see test_info_root.py.)
"""

from typing import Any, Dict

import pytest

from dbosify import workflow
from dbosify._internal import dispatcher
from dbosify.exceptions import TemporalError


@workflow.defn
class IntrospectionWorkflow:
    def __init__(self) -> None:
        self.done = False

    @workflow.update
    def echo_update_info(self, x: int) -> Dict[str, str]:
        info = workflow.current_update_info()
        assert info is not None
        return {"id": info.id, "name": info.name}

    @workflow.update
    def mutate_in_validator(self, x: int) -> str:
        return "unreachable"

    @mutate_in_validator.validator
    def _mutate_validator(self, x: int) -> None:
        # Mutating from a read-only validator must be rejected.
        workflow.upsert_memo({"k": "v"})

    @workflow.update
    def random_in_validator(self, x: int) -> str:
        return "unreachable"

    @random_in_validator.validator
    def _random_validator(self, x: int) -> None:
        # Consuming the shared RNG from a read-only validator must be rejected.
        workflow.random().random()

    @workflow.update
    def continue_as_new_in_validator(self, x: int) -> str:
        return "unreachable"

    @continue_as_new_in_validator.validator
    def _can_validator(self, x: int) -> None:
        # continue-as-new from a read-only validator must be rejected.
        workflow.continue_as_new()

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self) -> Dict[str, Any]:
        seed = workflow.random_seed()
        nr = workflow.new_random()
        result = {
            "is_self": workflow.instance() is self,
            "update_info_none_in_run": workflow.current_update_info() is None,
            "new_random_value": nr.randint(0, 10**9),
        }
        await workflow.wait_condition(lambda: self.done)
        result["seed_stable"] = workflow.random_seed() == seed
        return result


@pytest.mark.usefixtures("dbosify")
def test_instance_random_and_update_info() -> None:
    dispatcher.register_worker(workflows=[IntrospectionWorkflow])
    handle = dispatcher.start_workflow(
        IntrospectionWorkflow, [], workflow_id="introspect"
    )

    # current_update_info() resolves inside an update handler to that update.
    info = dispatcher.execute_update(
        "introspect", "echo_update_info", [1], update_id="upd-42"
    )
    assert info == {"id": "upd-42", "name": "echo_update_info"}

    # A validator that mutates state is rejected (read-only context).
    with pytest.raises(dispatcher.WorkflowUpdateFailedError):
        dispatcher.execute_update("introspect", "mutate_in_validator", [1])

    # random() and continue_as_new() are likewise rejected in a read-only context.
    with pytest.raises(dispatcher.WorkflowUpdateFailedError):
        dispatcher.execute_update("introspect", "random_in_validator", [1])
    with pytest.raises(dispatcher.WorkflowUpdateFailedError):
        dispatcher.execute_update("introspect", "continue_as_new_in_validator", [1])

    dispatcher.signal_workflow("introspect", "finish")
    result = dispatcher.workflow_result(handle)
    assert result["is_self"] is True
    assert result["update_info_none_in_run"] is True
    assert result["seed_stable"] is True
    assert isinstance(result["new_random_value"], int)


def test_read_only_context_error_is_temporal_error() -> None:
    assert issubclass(workflow.ReadOnlyContextError, TemporalError)
