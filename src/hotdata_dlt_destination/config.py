from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _parse_max_retries(value: str) -> int:
    try:
        n = int(value)
    except ValueError:
        raise ValueError(f"HOTDATA_MAX_RETRIES must be an integer, got {value!r}") from None
    if n < 1:
        raise ValueError(f"HOTDATA_MAX_RETRIES must be >= 1, got {n}")
    return n


def _parse_backoff(value: str) -> float:
    try:
        n = float(value)
    except ValueError:
        raise ValueError(f"HOTDATA_RETRY_BACKOFF_SECONDS must be a number, got {value!r}") from None
    if n < 0:
        raise ValueError(f"HOTDATA_RETRY_BACKOFF_SECONDS must be >= 0, got {n}")
    return n


def plain_env_overrides() -> dict[str, Any]:
    """Settings read from the plain ``HOTDATA_*`` environment variables.

    This is the documented environment contract, shared by the destination
    factory and the diagnostic CLI. Only variables that are set — and, for the
    string-valued ones, non-empty — appear in the result, so an absent key
    means "not configured this way" and the caller's default stands.
    """
    overrides: dict[str, Any] = {}
    for key, name in (
        ("database_id", "HOTDATA_DATABASE_ID"),
        ("database_name", "HOTDATA_DATABASE"),
        ("schema", "HOTDATA_SCHEMA"),
        ("write_disposition", "HOTDATA_WRITE_DISPOSITION"),
        ("api_base_url", "HOTDATA_API_BASE_URL"),
    ):
        value = os.environ.get(name)
        if value:
            overrides[key] = value
    declared = os.environ.get("HOTDATA_DECLARED_TABLES")
    if declared is not None:
        tables = [table.strip() for table in declared.split(",") if table.strip()]
        if tables:
            overrides["declared_tables"] = tables
    flag = os.environ.get("HOTDATA_CREATE_DATABASE_IF_MISSING")
    if flag is not None:
        overrides["create_database_if_missing"] = flag.lower() in {"1", "true", "yes"}
    retries = os.environ.get("HOTDATA_MAX_RETRIES")
    if retries is not None:
        overrides["max_retries"] = _parse_max_retries(retries)
    backoff = os.environ.get("HOTDATA_RETRY_BACKOFF_SECONDS")
    if backoff is not None:
        overrides["retry_backoff_seconds"] = _parse_backoff(backoff)
    return overrides


@dataclass(frozen=True)
class HotdataDestinationConfig:
    api_key: str
    database_name: str
    database_id: str | None = None
    api_base_url: str = "https://api.hotdata.dev"
    schema: str = "public"
    write_disposition: str = "append"
    create_database_if_missing: bool = True
    declared_tables: tuple[str, ...] = ()
    max_retries: int = 8
    retry_backoff_seconds: float = 1.5

    @classmethod
    def from_env(cls) -> HotdataDestinationConfig:
        overrides = plain_env_overrides()
        if "declared_tables" in overrides:
            overrides["declared_tables"] = tuple(overrides["declared_tables"])
        return cls(
            api_key=os.environ["HOTDATA_API_KEY"],
            database_name=overrides.pop("database_name", "dlt"),
            **overrides,
        )
