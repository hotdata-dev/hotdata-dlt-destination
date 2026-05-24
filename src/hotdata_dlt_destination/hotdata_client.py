from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar

import pyarrow as pa
from hotdata.api.query_api import QueryApi
from hotdata.api.query_runs_api import QueryRunsApi
from hotdata.api.results_api import ResultsApi
from hotdata.arrow import ResultsApi as ArrowResultsApi
from hotdata.models.async_query_response import AsyncQueryResponse
from hotdata.models.query_request import QueryRequest
from hotdata.models.query_response import QueryResponse
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

    def fetch_table(
        self,
        *,
        database: str,
        schema: str,
        table: str,
    ) -> pa.Table | None:
        """Fetch table contents as an Arrow table.

        Returns None when the table has never been loaded.
        Queries are scoped to the managed database via the X-Database-Id
        header; inside that scope the table is always at "default".<schema>.<table>.
        Uses Arrow IPC for the result download (avoiding JSON overhead).
        """
        def operation() -> pa.Table | None:
            if not self.table_is_synced(database, table, schema=schema):
                return None
            db = self._runtime.resolve_managed_database(database)
            sql = f'SELECT * FROM "default"."{schema}"."{table}"'
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return None
            return ArrowResultsApi(self._runtime.api).get_result_arrow(result_id)

        return self._request_with_retry(operation)

    _QUERY_TIMEOUT_SECONDS = 300.0

    def _query_database_scoped(self, sql: str, *, database_id: str) -> str | None:
        """Execute SQL scoped to a managed database; return the result_id.

        Passes the X-Database-Id header so that "default".<schema>.<table>
        resolves to the managed database's built-in catalog.  Polls until
        completion for async responses.
        """
        raw = QueryApi(self._runtime.api).query(
            QueryRequest(sql=sql),
            x_database_id=database_id,
        )
        if isinstance(raw, QueryResponse):
            return raw.result_id

        if isinstance(raw, AsyncQueryResponse):
            runs = QueryRunsApi(self._runtime.api)
            deadline = time.monotonic() + self._QUERY_TIMEOUT_SECONDS
            result_id: str | None = None
            while time.monotonic() < deadline:
                run = runs.get_query_run(raw.query_run_id)
                if run.status == "succeeded":
                    result_id = run.result_id
                    break
                if run.status in ("failed", "cancelled"):
                    raise RuntimeError(run.error_message or f"Query {run.status}")
                time.sleep(0.5)
            else:
                raise TimeoutError(
                    f"Managed database query timed out after {self._QUERY_TIMEOUT_SECONDS}s"
                )
            return self._wait_result_ready(result_id)

        return None

    def _wait_result_ready(self, result_id: str | None) -> str | None:
        """Poll result status until ready; return result_id once ready."""
        if result_id is None:
            return None
        results = ResultsApi(self._runtime.api)
        deadline = time.monotonic() + self._QUERY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            r = results.get_result(result_id)
            if r.status == "ready":
                return result_id
            if r.status in ("failed", "cancelled"):
                raise RuntimeError(r.error_message or f"Result {r.status}")
            time.sleep(0.3)
        raise TimeoutError(f"Result {result_id} not ready after {self._QUERY_TIMEOUT_SECONDS}s")

    def fetch_table_rows(
        self,
        *,
        database: str,
        schema: str,
        table: str,
    ) -> list[dict[str, Any]]:
        result = self.fetch_table(database=database, schema=schema, table=table)
        return result.to_pylist() if result is not None else []

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
                raise mapped_error from error

    @staticmethod
    def _classify_error(error: Exception) -> HotdataTerminalError | HotdataTransientError:
        return classify_sdk_error(error.__cause__ or error)
