from __future__ import annotations

import os
from dataclasses import dataclass


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
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> HotdataDestinationConfig:
        declared = os.environ.get("HOTDATA_DECLARED_TABLES", "")
        declared_tables = tuple(table.strip() for table in declared.split(",") if table.strip())
        return cls(
            api_key=os.environ["HOTDATA_API_KEY"],
            database_name=os.environ.get("HOTDATA_DATABASE", "dlt"),
            database_id=os.environ.get("HOTDATA_DATABASE_ID") or None,
            api_base_url=os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev"),
            schema=os.environ.get("HOTDATA_SCHEMA", "public"),
            write_disposition=os.environ.get("HOTDATA_WRITE_DISPOSITION", "append"),
            create_database_if_missing=os.environ.get(
                "HOTDATA_CREATE_DATABASE_IF_MISSING", "true"
            ).lower()
            in {"1", "true", "yes"},
            declared_tables=declared_tables,
            max_retries=_parse_max_retries(os.environ.get("HOTDATA_MAX_RETRIES", "5")),
            retry_backoff_seconds=_parse_backoff(
                os.environ.get("HOTDATA_RETRY_BACKOFF_SECONDS", "1.0")
            ),
        )
