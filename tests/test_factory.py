import pytest

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.job_client import HotdataJobClient

_ENV_KEYS = (
    "HOTDATA_API_KEY",
    "HOTDATA_WORKSPACE",
    "DESTINATION__HOTDATA__CREDENTIALS__API_KEY",
    "DESTINATION__HOTDATA__WORKSPACE_ID",
)


def _resolve(dest):
    return dest.configuration(dest.spec(), accept_partial=True)


@pytest.fixture
def clean_env(monkeypatch):
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_capabilities_use_parquet_and_nesting() -> None:
    caps = hotdata().capabilities()
    assert caps.preferred_loader_file_format == "parquet"
    assert caps.max_table_nesting == 1000
    assert caps.naming_convention == "snake_case"
    # Loads lock at the catalog (database) level: parallel loads of DIFFERENT
    # tables in one database 409 each other, so the loader must be fully
    # sequential by default.
    assert caps.loader_parallelism_strategy == "sequential"
    assert caps.supported_merge_strategies == ["upsert", "insert-only"]
    assert caps.supported_replace_strategies == ["truncate-and-insert"]


def test_capabilities_set_numeric_precision() -> None:
    # Without these, dlt's normalize crashes mapping a Decimal/wei column to parquet
    # (get_py_arrow_numeric indexes None). (38, 9) / (78, 0) are dlt's defaults.
    caps = hotdata().capabilities()
    assert caps.decimal_precision == (38, 9)
    assert caps.wei_precision == (78, 0)


def test_loader_parallelism_strategy_override() -> None:
    caps = hotdata(loader_parallelism_strategy="table-sequential").capabilities()
    assert caps.loader_parallelism_strategy == "table-sequential"


def test_max_table_nesting_override() -> None:
    caps = hotdata(max_table_nesting=2).capabilities()
    assert caps.max_table_nesting == 2


def test_client_class_is_job_client() -> None:
    assert hotdata().client_class is HotdataJobClient


def test_api_key_resolves_from_env(clean_env) -> None:
    # The API key (a secret) is read from the environment; workspace_id is a param.
    clean_env.setenv("HOTDATA_API_KEY", "sk_env")
    cfg = _resolve(hotdata(workspace_id="ws_param", database_name="d", declared_tables=["t"]))
    assert cfg.credentials.api_key == "sk_env"
    assert cfg.workspace_id == "ws_param"


def test_workspace_id_has_no_env_fallback(clean_env) -> None:
    # HOTDATA_WORKSPACE is not read — the workspace must be passed as a param.
    clean_env.setenv("HOTDATA_API_KEY", "sk_env")
    clean_env.setenv("HOTDATA_WORKSPACE", "ws_env")
    cfg = _resolve(hotdata(database_name="d", declared_tables=["t"]))
    assert cfg.workspace_id is None


def test_legacy_workspace_in_credentials_dict_is_hoisted_without_mutating(clean_env) -> None:
    creds = {"api_key": "k", "workspace_id": "ws_dict"}
    with pytest.warns(DeprecationWarning):
        dest = hotdata(credentials=creds, database_name="d", declared_tables=["t"])
    # the caller's dict is left intact (safe to reuse across hotdata(...) calls)
    assert creds == {"api_key": "k", "workspace_id": "ws_dict"}
    cfg = _resolve(dest)
    assert cfg.credentials.api_key == "k"
    assert cfg.workspace_id == "ws_dict"
