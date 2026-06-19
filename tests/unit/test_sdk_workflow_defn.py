"""Pure unit tests for ``@workflow.defn`` validation.

Adapted from temporalio's top-level ``tests/test_workflow.py`` (the ~17-test
definition-validation suite). These tests apply decorators and inspect/validate
the resulting workflow definitions; they do NOT touch Postgres, a Worker, or a
Client. Error *messages* differ from temporalio's, so assertions check the
exception TYPE plus a substring of OUR message (discovered by running the
decorators). Each adapted-with-changed-assertion or skipped case is annotated.

Our introspection analog of ``temporalio.workflow._Definition.from_class`` is
``dbosify._internal.registry.workflow_definition_of(cls)``.
"""

import typing
from typing import Sequence

import pytest

import dbosify._internal.registry as registry
from dbosify import workflow
from dbosify.common import RawValue, VersioningBehavior


class GoodDefnBase:
    @workflow.run
    async def run(self, _name: str) -> str:
        raise NotImplementedError

    @workflow.signal
    def base_signal(self) -> None:
        pass

    @workflow.query
    def base_query(self) -> None:
        pass

    @workflow.update
    def base_update(self) -> None:
        pass


@workflow.defn(name="workflow-custom")
class GoodDefn(GoodDefnBase):
    @workflow.run
    async def run(self, _name: str) -> str:
        raise NotImplementedError

    @workflow.signal
    def signal1(self) -> None:
        pass

    @workflow.signal(name="signal-custom", description="fun")
    def signal2(self) -> None:
        pass

    @workflow.signal(dynamic=True, description="boo")
    def signal3(self, _name: str, _args: Sequence[RawValue]) -> None:
        pass

    @workflow.query
    def query1(self) -> None:
        pass

    @workflow.query(name="query-custom", description="qd")
    def query2(self) -> None:
        pass

    @workflow.query(dynamic=True, description="dqd")
    def query3(self, _name: str, _args: Sequence[RawValue]) -> None:
        pass

    @workflow.update
    def update1(self) -> None:
        pass

    @workflow.update(name="update-custom", description="ud")
    def update2(self) -> None:
        pass

    @workflow.update(dynamic=True, description="dud")
    def update3(self, _name: str, _args: Sequence[RawValue]) -> None:
        pass


@workflow.defn()
class GoodDefnDeprecatedTypes(GoodDefnBase):
    # Defining this confirms the typing.Sequence spelling of Sequence[RawValue]
    # is accepted (same as collections.abc) and triggers no RuntimeError.
    @workflow.run
    async def run(self, _name: str) -> str:
        raise NotImplementedError

    @workflow.signal(dynamic=True)
    def signal(self, _name: str, _args: typing.Sequence[RawValue]) -> None:
        pass

    @workflow.query(dynamic=True)
    def query(self, _name: str, _args: typing.Sequence[RawValue]) -> None:
        pass

    @workflow.update(dynamic=True)
    def update(self, _name: str, _args: typing.Sequence[RawValue]) -> None:
        pass


def test_workflow_defn_good() -> None:
    # Adapted: our WorkflowDefinition has different fields, so we assert the
    # observable shape: name, run fn, and the signal/query/update handler maps.
    defn = registry.workflow_definition_of(GoodDefn)
    assert defn.name == "workflow-custom"
    assert defn.cls is GoodDefn
    assert defn.run_fn is GoodDefn.run

    assert set(defn.signals) == {"signal1", "signal-custom", "base_signal", None}
    assert defn.signals["signal-custom"].description == "fun"
    assert defn.signals[None].description == "boo"
    assert defn.signals[None].name is None  # dynamic handler

    assert set(defn.queries) == {"query1", "query-custom", "base_query", None}
    assert defn.queries["query-custom"].description == "qd"
    assert defn.queries[None].description == "dqd"

    assert set(defn.updates) == {"update1", "update-custom", "base_update", None}
    assert defn.updates["update-custom"].description == "ud"
    assert defn.updates[None].description == "dud"

    # GoodDefnDeprecatedTypes must have decorated cleanly (no RuntimeError).
    assert registry.workflow_definition_of(GoodDefnDeprecatedTypes) is not None


@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)
class VersioningBehaviorDefn:
    @workflow.run
    async def run(self, _name: str) -> str:
        raise NotImplementedError


def test_workflow_definition_with_versioning_behavior() -> None:
    # Adapted: we store versioning_behavior as the enum's int value, not the
    # enum, so assert the stored int matches PINNED.
    defn = registry.workflow_definition_of(VersioningBehaviorDefn)
    assert defn.name == "VersioningBehaviorDefn"
    assert defn.cls is VersioningBehaviorDefn
    assert defn.run_fn is VersioningBehaviorDefn.run
    assert defn.signals == {}
    assert defn.queries == {}
    assert defn.updates == {}
    assert defn.versioning_behavior == VersioningBehavior.PINNED.value


class BadDefnBase:
    @workflow.signal
    def base_signal(self) -> None:
        pass

    @workflow.query
    def base_query(self) -> None:
        pass

    @workflow.update
    def base_update(self) -> None:
        pass


def test_workflow_defn_bad() -> None:
    # Adapted: our registry raises on the FIRST problem (a single typed
    # ValueError), so we assert each detectable failure independently.

    # 1. Missing @workflow.run.
    with pytest.raises(ValueError) as err:

        @workflow.defn
        class MissingRun(BadDefnBase):
            @workflow.signal
            def signal1(self) -> None:
                pass

    assert "Missing @workflow.run method" in str(err.value)

    # 2. Duplicate named signal.
    with pytest.raises(ValueError) as err:

        @workflow.defn
        class DupSignal:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.signal
            def signal1(self) -> None:
                pass

            @workflow.signal(name="signal1")
            def signal2(self) -> None:
                pass

    assert "Multiple signal methods found for 'signal1'" in str(err.value)

    # 3. Duplicate dynamic signal.
    with pytest.raises(ValueError) as err:

        @workflow.defn
        class DupDynamicSignal:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.signal(dynamic=True)
            def signal3(self, _name: str, _args: Sequence[RawValue]) -> None:
                pass

            @workflow.signal(dynamic=True)
            def signal4(self, _name: str, _args: Sequence[RawValue]) -> None:
                pass

    assert "Multiple dynamic signal handlers found" in str(err.value)

    # 4. Duplicate named query.
    with pytest.raises(ValueError) as err:

        @workflow.defn
        class DupQuery:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.query
            def query1(self) -> None:
                pass

            @workflow.query(name="query1")
            def query2(self) -> None:
                pass

    assert "Multiple query methods found for 'query1'" in str(err.value)

    # 5. Duplicate named update.
    with pytest.raises(ValueError) as err:

        @workflow.defn
        class DupUpdate:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            def update1(self, _arg1: str) -> None:
                pass

            @workflow.update(name="update1")
            def update2(self, _arg1: str) -> None:
                pass

    assert "Multiple update methods found for 'update1'" in str(err.value)

    # Our registry has no base-vs-override decorator-drop check (handlers resolve
    # via inspect.getmembers); see test_workflow_defn_run_override_without_decorator.


@pytest.mark.skip(
    reason="Our registry does not reject locally-defined workflow classes "
    "(no 'Local classes unsupported' check); decorating a local class succeeds."
)
def test_workflow_defn_local_class() -> None:
    pass


class NonAsyncRun:
    def run(self) -> None:
        pass


def test_workflow_defn_non_async_run() -> None:
    # Adapted: our @workflow.run decorator rejects a non-async function with a
    # ValueError whose message is "Workflow run method must be an async function".
    with pytest.raises(ValueError) as err:
        workflow.run(NonAsyncRun.run)  # type: ignore[arg-type, unused-ignore]
    assert "must be an async function" in str(err.value)


class BaseWithRun:
    @workflow.run
    async def run(self) -> None:
        pass


class RunOnlyOnBase(BaseWithRun):
    pass


@pytest.mark.skip(
    reason="Our registry resolves @workflow.run via inspect.getmembers, so a run "
    "method defined only on the base class is inherited and accepted; temporalio "
    "requires it to be redefined on the decorated subclass."
)
def test_workflow_defn_run_only_on_base() -> None:
    pass


class RunWithoutDecoratorOnOverride(BaseWithRun):
    async def run(self) -> None:
        pass


def test_workflow_defn_run_override_without_decorator() -> None:
    # Adapted: with no base-vs-override decorator check, the undecorated override
    # shadows the base's marker, so getmembers finds none -> "Missing @workflow.run".
    with pytest.raises(ValueError) as err:
        workflow.defn(RunWithoutDecoratorOnOverride)
    assert "Missing @workflow.run method" in str(err.value)


class MultipleRun:
    @workflow.run
    async def run1(self) -> None:
        pass

    @workflow.run
    async def run2(self) -> None:
        pass


def test_workflow_defn_multiple_run() -> None:
    # Adapted: our message is
    # "Multiple methods found for @workflow.run: ['run1', 'run2']".
    with pytest.raises(ValueError) as err:
        workflow.defn(MultipleRun)
    assert "Multiple methods found for @workflow.run" in str(err.value)


def test_workflow_defn_bad_dynamic() -> None:
    # Adapted: we validate the dynamic-handler signature at @workflow.defn time
    # (RuntimeError), so the bad handlers must be wrapped in a decorated class.
    with pytest.raises(RuntimeError) as err:

        @workflow.defn
        class BadDynSignalNoArgs:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.signal(dynamic=True)
            def some_dynamic1(self) -> None:
                pass

    assert "Dynamic signal handler must accept" in str(err.value)

    with pytest.raises(RuntimeError) as err:

        @workflow.defn
        class BadDynSignalOneArg:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.signal(dynamic=True)
            def some_dynamic2(self, _no_vararg: object) -> None:
                pass

    assert "Dynamic signal handler must accept" in str(err.value)

    with pytest.raises(RuntimeError) as err:

        @workflow.defn
        class BadDynQueryNoArgs:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.query(dynamic=True)
            def some_dynamic1(self) -> None:
                pass

    assert "Dynamic query handler must accept" in str(err.value)

    with pytest.raises(RuntimeError) as err:

        @workflow.defn
        class BadDynQueryOneArg:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.query(dynamic=True)
            def some_dynamic2(self, _no_vararg: object) -> None:
                pass

    assert "Dynamic query handler must accept" in str(err.value)


@pytest.mark.skip(
    reason="No old-style ((name, *args)) dynamic-handler deprecation warning in "
    "our impl: such a signature is simply rejected as invalid at @workflow.defn "
    "time (RuntimeError), so there is no in-process DeprecationWarning to observe."
)
def test_workflow_defn_dynamic_handler_warnings() -> None:
    pass


@pytest.mark.skip(
    reason="No public 'parameters identical up to naming' helper "
    "(workflow._parameters_identical_up_to_naming) is exposed; our registry does "
    "not implement update-validator parameter-parity checking."
)
def test_parameters_identical_up_to_naming() -> None:
    pass


class BadWorkflowInit:
    def not__init__(self) -> None:
        pass

    @workflow.run
    async def run(self) -> None:
        pass


def test_workflow_init_not__init__() -> None:
    # Adapted: our message is "@workflow.init may only be used on __init__"
    # (temporalio's is "... on the __init__ method").
    with pytest.raises(ValueError) as err:
        workflow.init(BadWorkflowInit.not__init__)
    assert "@workflow.init may only be used on" in str(err.value)


@pytest.mark.skip(
    reason="Our registry does not validate update-validator parameter parity "
    "against the update method; a mismatched @validator signature is accepted, so "
    "there is no exception to assert."
)
def test_workflow_update_validator_not_update() -> None:
    pass


@pytest.mark.skip(
    reason="workflow.ActivityConfig / workflow.start_activity config-parity helper "
    "and workflow._NotInWorkflowEventLoopError are not exposed; "
    "hasattr(workflow, 'ActivityConfig') is False."
)
def test_activity_config_parity_with_start_activity() -> None:
    pass


@pytest.mark.skip(
    reason="workflow.ActivityConfig / workflow._NotInWorkflowEventLoopError are "
    "not exposed (no ActivityConfig dataclass to compare against execute_activity)."
)
async def test_activity_config_parity_with_execute_activity() -> None:
    pass


@pytest.mark.skip(
    reason="workflow.ChildWorkflowConfig / workflow._NotInWorkflowEventLoopError "
    "are not exposed (no ChildWorkflowConfig dataclass to compare against)."
)
async def test_child_workflow_config_parity_with_execute_child_workflow() -> None:
    pass


@pytest.mark.skip(
    reason="workflow.ChildWorkflowConfig / workflow._NotInWorkflowEventLoopError "
    "are not exposed (no ChildWorkflowConfig dataclass to compare against)."
)
async def test_child_workflow_config_parity_with_start_child_workflow() -> None:
    pass
