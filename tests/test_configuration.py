import pytest
from dlt.common.configuration.exceptions import ConfigurationValueError

from hotdata_dlt_destination.configuration import (
    HotdataClientConfiguration,
    HotdataCredentials,
    validate_credentials,
)


def _config(*, api_key: str | None, workspace_id: str | None) -> HotdataClientConfiguration:
    return HotdataClientConfiguration(
        credentials=HotdataCredentials(api_key=api_key),
        workspace_id=workspace_id,
        database_name="db",
        schema="public",
    )


def test_validate_passes_when_api_key_and_workspace_set() -> None:
    validate_credentials(_config(api_key="k", workspace_id="ws"))


def test_validate_raises_when_api_key_missing() -> None:
    with pytest.raises(ConfigurationValueError, match="api_key"):
        validate_credentials(_config(api_key=None, workspace_id="ws"))


def test_validate_raises_when_credentials_missing() -> None:
    cfg = HotdataClientConfiguration(credentials=None, workspace_id="ws", database_name="db")
    with pytest.raises(ConfigurationValueError, match="api_key"):
        validate_credentials(cfg)


def test_validate_raises_when_workspace_missing() -> None:
    with pytest.raises(ConfigurationValueError, match="workspace_id"):
        validate_credentials(_config(api_key="k", workspace_id=None))


def test_validate_reports_both_missing_fields() -> None:
    with pytest.raises(ConfigurationValueError, match=r"api_key.*workspace_id"):
        validate_credentials(_config(api_key=None, workspace_id=None))
