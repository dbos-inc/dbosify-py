"""Worker-versioning value types (no database). These mirror
``temporalio.common`` for parity; their behavior is inert in dbosify
(DEVIATIONS worker-versioning)."""

import pytest

from dbosify.common import (
    AutoUpgradeVersioningOverride,
    PinnedVersioningOverride,
    VersioningBehavior,
    VersioningOverride,
    WorkerDeploymentVersion,
)


def test_versioning_behavior_values() -> None:
    assert VersioningBehavior.UNSPECIFIED.value == 0
    assert VersioningBehavior.PINNED.value == 1
    assert VersioningBehavior.AUTO_UPGRADE.value == 2


def test_worker_deployment_version_canonical_roundtrip() -> None:
    v = WorkerDeploymentVersion(deployment_name="my-app", build_id="0.1")
    assert v.to_canonical_string() == "my-app.0.1"
    # build_id may itself contain dots; deployment name is split off once.
    parsed = WorkerDeploymentVersion.from_canonical_string("my-app.0.1")
    assert parsed == WorkerDeploymentVersion("my-app", "0.1")


def test_worker_deployment_version_from_canonical_requires_separator() -> None:
    with pytest.raises(ValueError):
        WorkerDeploymentVersion.from_canonical_string("no-dot")


def test_versioning_overrides_are_versioning_override() -> None:
    v = WorkerDeploymentVersion("dep", "build")
    assert isinstance(PinnedVersioningOverride(version=v), VersioningOverride)
    assert isinstance(AutoUpgradeVersioningOverride(), VersioningOverride)
    assert PinnedVersioningOverride(version=v).version is v
