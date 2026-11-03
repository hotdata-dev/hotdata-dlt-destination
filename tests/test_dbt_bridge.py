"""The dbt bridge: profile wiring, database-id resolution, runner arguments."""

from __future__ import annotations

import os
import re
from pathlib import Path
from types import SimpleNamespace

import dlt
import pytest
import yaml
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.utils import add_config_to_env

from hotdata_dlt_destination import dbt as bridge
from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import (
    HotdataClientConfiguration,
    HotdataCredentials,
)

PROFILE_PATH = Path(bridge.PROFILES_DIR) / "profiles.yml"


def _populated_config() -> HotdataClientConfiguration:
    cfg = HotdataClientConfiguration()
    cfg.credentials = HotdataCredentials()
    cfg.credentials.api_key = "sk_test"
    cfg.workspace_id = "ws_test"
    cfg.database_id = "db_test"
    return cfg


def test_profile_parses_and_pins_the_database() -> None:
    profiles = yaml.safe_load(PROFILE_PATH.read_text())
    output = profiles["hotdata"]["outputs"]["analytics"]
    assert output["type"] == "hotdata"
    assert output["create_database_if_missing"] is False
    # the catalog inside an instant database is always "default"; the adapter
    # rejects an explicit override, so the profile must not set one
    assert "database" not in output
    assert output["threads"] == 1


def test_profile_env_vars_match_what_the_runner_exports() -> None:
    """Every DLT__* the profile reads must be exported by add_config_to_env."""
    referenced = set(re.findall(r"env_var\('(DLT__[A-Z0-9_]+)'", PROFILE_PATH.read_text()))
    assert referenced  # the profile reads the environment at all

    before = dict(os.environ)
    try:
        add_config_to_env(_populated_config(), ("dlt",))
        exported = set(os.environ) - set(before)
    finally:
        os.environ.clear()
        os.environ.update(before)
    assert referenced <= exported


def test_resolve_database_id_prefers_the_pin() -> None:
    cfg = _populated_config()
    cfg._hotdata_db = SimpleNamespace(id="db_cached")
    assert bridge._resolve_database_id(cfg) == "db_test"


def test_resolve_database_id_falls_back_to_the_run_cache() -> None:
    cfg = _populated_config()
    cfg.database_id = None
    assert bridge._resolve_database_id(cfg) is None
    cfg._hotdata_db = SimpleNamespace(id="db_cached")
    assert bridge._resolve_database_id(cfg) == "db_cached"


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.setenv("HOTDATA_API_KEY", "sk_test")
    return monkeypatch


def _pipeline(tmp_path: Path, name: str, **dest_kwargs: object) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=name,
        pipelines_dir=str(tmp_path),
        destination=hotdata(**dest_kwargs),
    )


def test_package_wires_the_runner(api_key: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_create_runner(venv, credentials, working_dir, package_location, **kwargs):
        captured.update(
            venv=venv,
            credentials=credentials,
            working_dir=working_dir,
            package_location=package_location,
            **kwargs,
        )
        return "the-runner"

    api_key.setattr(bridge, "create_runner", fake_create_runner)
    pipeline = _pipeline(tmp_path, "dbt_bridge_wires", workspace_id="ws", database_id="db_pin")

    assert bridge.package(pipeline, "/some/dbt/package") == "the-runner"

    run_config = captured["credentials"]
    assert isinstance(run_config, HotdataClientConfiguration)
    assert run_config.database_id == "db_pin"
    assert run_config.create_database_if_missing is False
    # create_runner reads `dataset_name` as dbt's source_dataset_name var; the
    # bridge maps it from the destination's schema
    assert run_config.dataset_name == run_config.schema == "public"
    assert captured["package_location"] == "/some/dbt/package"
    assert captured["package_profile_name"] == "hotdata"
    assert Path(str(captured["package_profiles_dir"]), "profiles.yml").is_file()
    assert captured["venv"] is None


def test_package_without_a_database_id_raises(api_key: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path, "dbt_bridge_no_id", workspace_id="ws", database_name="d")
    with pytest.raises(ConfigurationValueError, match="database_id"):
        bridge.package(pipeline, "/some/dbt/package")


def test_package_rejects_other_destinations(tmp_path: Path) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="dbt_bridge_wrong_dest",
        pipelines_dir=str(tmp_path),
        destination="dummy",
    )
    with pytest.raises(ConfigurationValueError, match="hotdata destination"):
        bridge.package(pipeline, "/some/dbt/package")


def test_dlt_derives_the_adapter_package_from_the_destination_name() -> None:
    """get_venv relies on dlt turning destination "hotdata" into pip "dbt-hotdata"."""
    from dlt.helpers.dbt import _create_dbt_deps

    deps = _create_dbt_deps(["hotdata"])
    assert any(dep.startswith("dbt-hotdata") for dep in deps)
