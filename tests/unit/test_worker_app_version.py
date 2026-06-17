"""The Worker pins a stable default DBOS ``application_version`` so redeploys
don't strand in-flight workflows, cooperating workers agree on a version, and
``workflow.patched()`` reaches pre-patch runs (DESIGN §6.8 / DEVIATIONS D27).
These cover the resolution of ``_with_default_app_version`` without launching
DBOS. The override is config-only — there is no ``DBOS__APPVERSION`` special
case.
"""

from dbos import DBOSConfig

from temporal_dbos.worker import DEFAULT_APP_VERSION, _with_default_app_version


def test_pins_default_when_version_unset() -> None:
    config: DBOSConfig = {"name": "app"}
    assert (
        _with_default_app_version(config)["application_version"] == DEFAULT_APP_VERSION
    )


def test_explicit_config_version_wins() -> None:
    config: DBOSConfig = {"name": "app", "application_version": "9.9"}
    assert _with_default_app_version(config)["application_version"] == "9.9"


def test_explicit_none_opts_into_dbos_autoversioning() -> None:
    # Passing the key explicitly as None means "let DBOS auto-compute from code".
    config: DBOSConfig = {"name": "app", "application_version": None}
    assert _with_default_app_version(config)["application_version"] is None
