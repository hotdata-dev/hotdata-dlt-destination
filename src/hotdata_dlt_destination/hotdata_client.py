from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar

from hotdata_runtime.client import HotdataClient as RuntimeClient
from hotdata_runtime.databases import LoadManagedTableResult, ManagedDatabase

from hotdata_dlt_destination.errors import (
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)

T = TypeVar("T")


class HotdataClient:
    """Managed-database client with bounded retries over hotdata-runtime."""

    def __init__(
        self,
        *,
        api_key: str,
        workspace_id: str,
        api_base_url: str,
        max_retries: int,
        retry_backoff_seconds: float,
    ) -> None:
        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._runtime = RuntimeClient(
            api_key,
            workspace_id,
            host=api_base_url.rstrip("/"),
        )

    def close(self) -> None:
        self._runtime.close()

    def ensure_managed_database(
        self,
        name: str,
        *,
        schema: str,
        tables: list[str],
        create_if_missing: bool,
    ) -> ManagedDatabase:
        def operation() -> ManagedDatabase:
            try:
                return self._runtime.resolve_managed_database(name)
            except KeyError:
                if not create_if_missing:
                    raise
                return self._runtime.create_managed_database(
                    description=name,
                    schema=schema,
                    tables=sorted(set(tables)),
                )

        return self._request_with_retry(operation)

    def table_is_synced(
        self,
        database: str,
        table: str,
        *,
        schema: str,
    ) -> bool:
        for managed_table in self._runtime.list_managed_tables(database, schema=schema):
            if managed_table.table == table:
                return managed_table.synced
        return False

    def fetch_table_rows(
        self,
        *,
        database: str,
        schema: str,
        table: str,
    ) -> list[dict[str, Any]]:
        def operation() -> list[dict[str, Any]]:
            if not self.table_is_synced(database, table, schema=schema):
                return []
            qualified_table = f'"{database}"."{schema}"."{table}"'
            result = self._runtime.execute_sql(f"SELECT * FROM {qualified_table}")
            return result.to_records()

        return self._request_with_retry(operation)

    def upload_parquet(self, path: str) -> str:
        return self._request_with_retry(lambda: self._runtime.upload_parquet(path))

    def load_managed_table(
        self,
        database: str,
        table: str,
        *,
        schema: str,
        upload_id: str,
    ) -> LoadManagedTableResult:
        return self._request_with_retry(
            lambda: self._runtime.load_managed_table(
                database,
                table,
                schema=schema,
                upload_id=upload_id,
            )
        )

    _MAX_BACKOFF_SECONDS = 30.0

    def _request_with_retry(self, operation: Callable[[], T]) -> T:
        for attempt in range(1, self._max_retries + 1):
            try:
                return operation()
            except Exception as error:
                mapped_error = self._classify_error(error)
                if isinstance(mapped_error, HotdataTransientError) and attempt < self._max_retries:
                    backoff = min(self._retry_backoff_seconds * attempt, self._MAX_BACKOFF_SECONDS)
                    time.sleep(backoff)
                    continue
                if isinstance(mapped_error, HotdataTransientError):
                    raise mapped_error from error
                raise mapped_error from error

    @staticmethod
    def _classify_error(error: Exception) -> HotdataTerminalError | HotdataTransientError:
        return classify_sdk_error(error.__cause__ or error)
