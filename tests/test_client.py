from types import SimpleNamespace

import pytest

from hotdata_dlt_destination.hotdata_client import HotdataClient


def test_upload_and_load_managed_table() -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.upload_calls = 0
            self.load_calls = 0

        def upload_parquet(self, path: str) -> str:
            self.upload_calls += 1
            assert path.endswith(".parquet")
            return "upload_1"

        def load_managed_table(
            self,
            database: str,
            table: str,
            *,
            schema: str,
            upload_id: str,
            mode: str = "replace",
            key: list[str] | None = None,
        ) -> SimpleNamespace:
            self.load_calls += 1
            assert database == "dlt"
            assert table == "orders"
            assert schema == "public"
            assert upload_id == "upload_1"
            return SimpleNamespace(
                connection_id="conn_1",
                schema_name=schema,
                table_name=table,
                row_count=1,
                full_name=f"{database}.{schema}.{table}",
            )

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=2,
        retry_backoff_seconds=0.0,
    )
    fake_runtime = FakeRuntime()
    client._runtime = fake_runtime

    upload_id = client.upload_parquet("/tmp/batch.parquet")
    loaded = client.load_managed_table(
        "dlt",
        "orders",
        schema="public",
        upload_id=upload_id,
    )

    assert upload_id == "upload_1"
    assert loaded.full_name == "dlt.public.orders"
    assert fake_runtime.upload_calls == 1
    assert fake_runtime.load_calls == 1
    client.close()


def test_fetch_table_rows_skips_unsynced_tables() -> None:
    class FakeRuntime:
        def list_managed_tables(self, database: str, *, schema: str):
            assert database == "dlt"
            assert schema == "public"
            return [SimpleNamespace(table="orders", synced=False)]

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = FakeRuntime()

    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == []
    client.close()


def test_fetch_table_rows_reads_synced_table() -> None:
    # Patch module-level QueryApi and ArrowResultsApi so no real HTTP happens.
    # The client implementation now lives in hotdata_framework.managed_client
    # (re-exported here as HotdataClient), so patch the symbols there.
    import hotdata_framework.managed_client as _mod
    import pyarrow as pa
    from hotdata.models.query_response import QueryResponse as _QR

    class FakeQueryApi:
        def __init__(self, api):
            pass

        def query(self, request, *, x_database_id):
            assert x_database_id == "db_1"
            assert 'SELECT * FROM "default"."public"."orders"' in request.sql
            return _QR(
                columns=["id", "name"],
                rows=[[1, "alpha"]],
                row_count=1,
                preview_row_count=1,
                truncated=False,
                nullable=[False, False],
                result_id="result_1",
                query_run_id="qrun_1",
                execution_time_ms=1,
            )

    class FakeResultsApi:
        def __init__(self, api):
            pass

        def get_result(self, result_id, *, x_database_id=None):
            assert result_id == "result_1"
            return SimpleNamespace(status="ready", result_id=result_id, error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api):
            pass

        # Required in the hotdata 0.6.0 SDK; framework >=0.6.1 passes it on
        # every result read.
        def get_result_arrow(self, result_id, *, x_database_id):
            assert result_id == "result_1"
            assert x_database_id == "db_1"
            return pa.table({"id": [1], "name": ["alpha"]})

    class FakeRuntime:
        api = None

        def list_managed_tables(self, database: str, *, schema: str):
            return [SimpleNamespace(table="orders", synced=True)]

        def resolve_managed_database(self, name):
            return SimpleNamespace(id="db_1")

        def close(self) -> None:
            return None

    orig_query_api = _mod.QueryApi
    orig_results_api = _mod.ResultsApi
    orig_arrow_api = _mod.ArrowResultsApi
    _mod.QueryApi = FakeQueryApi
    _mod.ResultsApi = FakeResultsApi
    _mod.ArrowResultsApi = FakeArrowResultsApi

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = FakeRuntime()

    try:
        rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
        assert rows == [{"id": 1, "name": "alpha"}]
    finally:
        _mod.QueryApi = orig_query_api
        _mod.ResultsApi = orig_results_api
        _mod.ArrowResultsApi = orig_arrow_api
    client.close()


def _patch_query_apis(monkeypatch, arrow_table, *, arrow_in_hotdata_client: bool) -> dict:
    """Patch the query/result APIs so no real HTTP happens.

    ``fetch_table`` resolves ArrowResultsApi from hotdata_framework.managed_client;
    ``execute_sql`` resolves it from hotdata_dlt_destination.hotdata_client.
    Returns a recorder of the ``x_database_id`` scopes each read carried —
    the hotdata 0.6.0 SDK requires the scope on ``get_result_arrow``, and
    framework >=0.6.1 / ``execute_sql`` must pass it on every result read.
    """
    import pyarrow as pa  # noqa: F401  (arrow_table already built by caller)
    from hotdata.models.query_response import QueryResponse as _QR

    scopes: dict[str, list] = {"result": [], "arrow": []}

    class FakeQueryApi:
        def __init__(self, api):
            pass

        def query(self, request, *, x_database_id):
            return _QR(
                columns=["id"],
                rows=[[1]],
                row_count=1,
                preview_row_count=1,
                truncated=False,
                nullable=[False],
                result_id="r1",
                query_run_id="q1",
                execution_time_ms=1,
            )

    class FakeResultsApi:
        def __init__(self, api):
            pass

        def get_result(self, result_id, *, x_database_id=None):
            scopes["result"].append(x_database_id)
            return SimpleNamespace(status="ready", result_id=result_id, error_message=None)

    class FakeArrowResultsApi:
        def __init__(self, api):
            pass

        def get_result_arrow(self, result_id, *, x_database_id):
            scopes["arrow"].append(x_database_id)
            return arrow_table

    monkeypatch.setattr("hotdata_framework.managed_client.QueryApi", FakeQueryApi)
    monkeypatch.setattr("hotdata_framework.managed_client.ResultsApi", FakeResultsApi)
    monkeypatch.setattr("hotdata_framework.managed_client.ArrowResultsApi", FakeArrowResultsApi)
    if arrow_in_hotdata_client:
        monkeypatch.setattr(
            "hotdata_dlt_destination.hotdata_client.ArrowResultsApi", FakeArrowResultsApi
        )
    return scopes


def _client_with_runtime(runtime) -> HotdataClient:
    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = runtime
    return client


def test_fetch_table_carries_database_scope(monkeypatch) -> None:
    # Hosted result endpoints reject requests without the database scope;
    # fetch_table (merge / state-sync read-back) must carry it on the result
    # poll and the Arrow fetch (native x_database_id since framework 0.6.1).
    import pyarrow as pa

    scopes = _patch_query_apis(monkeypatch, pa.table({"id": [1]}), arrow_in_hotdata_client=False)

    class FakeRuntime:
        api = None

        def list_managed_tables(self, database, *, schema):
            return [SimpleNamespace(table="orders", synced=True)]

        def resolve_managed_database(self, name):
            return SimpleNamespace(id="db_42")

        def close(self):
            return None

    client = _client_with_runtime(FakeRuntime())
    table = client.fetch_table(database="dlt", schema="public", table="orders")
    assert table is not None and table.num_rows == 1
    assert scopes["result"] == ["db_42"]
    assert scopes["arrow"] == ["db_42"]
    client.close()


def test_execute_sql_carries_database_scope(monkeypatch) -> None:
    # The read/dataset path carries the scope too.
    import pyarrow as pa

    scopes = _patch_query_apis(monkeypatch, pa.table({"id": [1, 2]}), arrow_in_hotdata_client=True)

    class FakeRuntime:
        api = None

        def resolve_managed_database(self, name):
            return SimpleNamespace(id="db_99")

        def close(self):
            return None

    client = _client_with_runtime(FakeRuntime())
    table = client.execute_sql('SELECT * FROM "default"."public"."spans"', database="dlt")
    assert table.num_rows == 2
    assert scopes["result"] == ["db_99"]
    assert scopes["arrow"] == ["db_99"]
    client.close()


def test_fetch_table_rows_returns_empty_when_table_missing() -> None:
    class FakeRuntime:
        def list_managed_tables(self, database: str, *, schema: str):
            return []

        def close(self) -> None:
            return None

    client = HotdataClient(
        api_key="k",
        workspace_id="ws_1",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = FakeRuntime()

    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == []
    client.close()


# --- ensure_managed_database / drop_managed_database (schema evolution) ---


class _EvoRuntime:
    """Fake runtime tracking the managed-database lifecycle calls."""

    def __init__(self, existing_db, existing_tables) -> None:
        self._existing_db = existing_db  # SimpleNamespace(id=...) or None
        self._existing_tables = list(existing_tables)
        self.created: list[tuple] = []
        self.deleted: list[str] = []
        self.added: list[tuple] = []
        self.uploaded: list[str] = []
        self.loaded: list[tuple] = []

    def resolve_managed_database(self, name):
        if self._existing_db is None:
            raise KeyError(name)
        return self._existing_db

    def list_managed_tables(self, name, *, schema):
        return [SimpleNamespace(table=t) for t in self._existing_tables]

    def add_managed_table(self, database, table, *, schema, key=None):
        self.added.append((database, table, schema))
        self._existing_tables.append(table)
        return SimpleNamespace(table=table, schema=schema)

    def create_managed_database(self, *, description, schema, tables, keys=None):
        self.created.append((description, schema, list(tables)))
        return SimpleNamespace(id="new_db")

    def delete_managed_database(self, db_id):
        self.deleted.append(db_id)

    def upload_parquet(self, path):
        self.uploaded.append(path)
        return "up_1"

    def load_managed_table(self, database, table, *, schema, upload_id, mode="replace", key=None):
        self.loaded.append((database, table, schema, upload_id))
        return SimpleNamespace()

    def close(self):
        return None


def _client_with(runtime) -> HotdataClient:
    client = HotdataClient(
        api_key="k",
        workspace_id="ws",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = runtime
    return client


def test_ensure_creates_when_missing() -> None:
    rt = _EvoRuntime(existing_db=None, existing_tables=[])
    client = _client_with(rt)
    client.ensure_managed_database("db", schema="public", tables=["orders"], create_if_missing=True)
    assert rt.created == [("db", "public", ["orders"])]
    assert rt.deleted == []
    client.close()


def test_ensure_raises_when_missing_and_no_create() -> None:
    rt = _EvoRuntime(existing_db=None, existing_tables=[])
    client = _client_with(rt)
    with pytest.raises(KeyError):
        client.ensure_managed_database(
            "db", schema="public", tables=["orders"], create_if_missing=False
        )
    client.close()


def test_ensure_noop_when_all_tables_present() -> None:
    rt = _EvoRuntime(
        existing_db=SimpleNamespace(id="db_1"), existing_tables=["orders", "customers"]
    )
    client = _client_with(rt)
    client.ensure_managed_database("db", schema="public", tables=["orders"], create_if_missing=True)
    assert rt.deleted == []
    assert rt.created == []
    client.close()


def test_ensure_adds_missing_table_without_recreate() -> None:
    rt = _EvoRuntime(existing_db=SimpleNamespace(id="db_1"), existing_tables=["orders"])
    client = _client_with(rt)

    client.ensure_managed_database(
        "db", schema="public", tables=["orders", "customers"], create_if_missing=True
    )

    # The missing table is declared in place; the database is never deleted or
    # recreated and no data is moved.
    assert rt.added == [("db", "customers", "public")]
    assert rt.deleted == []
    assert rt.created == []
    assert rt.uploaded == []
    assert rt.loaded == []
    client.close()


def test_drop_managed_database_deletes_when_present() -> None:
    rt = _EvoRuntime(existing_db=SimpleNamespace(id="db_1"), existing_tables=[])
    client = _client_with(rt)
    client.drop_managed_database("db")
    assert rt.deleted == ["db_1"]
    client.close()


def test_drop_managed_database_noop_when_absent() -> None:
    rt = _EvoRuntime(existing_db=None, existing_tables=[])
    client = _client_with(rt)
    client.drop_managed_database("db")
    assert rt.deleted == []
    client.close()
