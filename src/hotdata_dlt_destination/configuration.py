from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import ClassVar, Final

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import known_sections
from dlt.common.destination.client import (
    CredentialsConfiguration,
    DestinationClientConfiguration,
)


@configspec
class HotdataCredentials(CredentialsConfiguration):
    api_key: str | None = None
    """Hotdata API key (the secret / auth token)."""

    def __str__(self) -> str:
        return "hotdata://***" if self.api_key else "hotdata://<unset>"


@configspec
class HotdataClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[assignment]
        default="hotdata", init=False, repr=False, compare=False
    )
    credentials: HotdataCredentials = None

    workspace_id: str | None = None
    """Hotdata workspace ID. Pass as a ``hotdata(workspace_id=...)`` param."""
    api_base_url: str = "https://api.hotdata.dev"
    database_id: str | None = None
    """Id of the instant database to load into. This is how an existing database
    is targeted — Hotdata database names are not unique, so the id (not the name)
    is the identifier. Printed on first-run create; pin it to reuse the database."""
    database_name: str = "dlt"
    """Display label for the instant database, used only when creating a new one
    (never to look one up). Pin ``database_id`` to reuse an existing database."""
    schema: str = "public"
    """Schema within the instant database."""
    write_disposition: str = "append"
    """Default write disposition when not set on the resource."""
    declared_tables: list[str] | None = None
    """Explicit list of table names for multi-table pipelines."""
    create_database_if_missing: bool = True
    """Create the instant database automatically if it does not exist."""
    max_retries: int = 8
    """Retry budget for transient API errors (409/429/5xx). Loads take a
    catalog-level lock per database, so a concurrent writer can hold
    409s for tens of seconds — the budget must outlast
    that, not just blips. 8 attempts x 1.5s linear backoff ~= 42s."""
    retry_backoff_seconds: float = 1.5
    max_state_files: int = 100
    """How many `_dlt_pipeline_state` rows to keep per pipeline.

    An incremental pipeline writes one state row per run, and only the newest
    matching row is ever read — the rest are history the destination never
    consults. Set to 0 (or less) to keep every row."""
    max_table_nesting: int | None = None
    """Override the default maximum table nesting depth."""
    loader_parallelism_strategy: str | None = None
    """Override the default loader parallelism strategy (e.g. 'table-sequential')."""

    __config_gen_annotations__: ClassVar[list[str]] = []
    __recommended_sections__: ClassVar[Sequence[str]] = (known_sections.DESTINATION, "hotdata", "")

    def __str__(self) -> str:
        return f"hotdata://{self.workspace_id}"


def validate_credentials(config: HotdataClientConfiguration) -> None:
    """Raise an actionable error when the fields needed to reach the API are unset."""
    missing = []
    if config.credentials is None or not config.credentials.api_key:
        missing.append("api_key (set HOTDATA_API_KEY or pass credentials=)")
    if not config.workspace_id:
        missing.append("workspace_id (pass workspace_id= to hotdata(...))")
    if missing:
        raise ConfigurationValueError(
            "hotdata destination is missing required configuration: " + "; ".join(missing)
        )
