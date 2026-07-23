from types import SimpleNamespace

import pytest

from hotdata_dlt_destination.errors import HotdataTerminalError
from hotdata_dlt_destination.hotdata_client import HotdataClient


def _db(db_id: str, name: str, conn: str = "conn") -> SimpleNamespace:
    return SimpleNamespace(id=db_id, description=name, default_connection_id=conn)


def _client(runtime, *, cache=None) -> HotdataClient:
    client = HotdataClient(
        api_key="k",
        workspace_id="ws",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = runtime
    if cache is not None:
        client.bind_run_cache(cache)
    return client


# --- resolution: collision-safe, id-addressed, resolve-once ---------------


def test_resolve_returns_single_match_by_name() -> None:
    class FakeRuntime:
        def list_managed_databases(self):
            return [_db("db_1", "dlt")]

        def close(self):
            return None

    client = _client(FakeRuntime())
    assert client.resolve_managed_database("dlt").id == "db_1"
    client.close()


def test_resolve_by_id() -> None:
    class FakeRuntime:
        def list_managed_databases(self):
            return [_db("db_1", "dlt")]

        def close(self):
            return None

    client = _client(FakeRuntime())
    assert client.resolve_managed_database("db_1").id == "db_1"
    client.close()


def test_resolve_missing_raises_keyerror() -> None:
    class FakeRuntime:
        def list_managed_databases(self):
            return []

        def close(self):
            return None

    client = _client(FakeRuntime())
    with pytest.raises(KeyError):
        client.resolve_managed_database("dlt")
    client.close()


def test_resolve_raises_on_ambiguous_name() -> None:
    # Hotdata names are not unique: >1 match must raise, never silently pick one.
    class FakeRuntime:
        def list_managed_databases(self):
            return [_db("db_1", "dlt"), _db("db_2", "dlt")]

        def close(self):
            return None

    client = _client(FakeRuntime())
    with pytest.raises(HotdataTerminalError, match="ambiguous"):
        client.resolve_managed_database("dlt")
    client.close()


def test_resolves_once_and_reuses_run_cache() -> None:
    # The name is resolved once per run; a second client sharing the cache reuses it.
    class FakeRuntime:
        def __init__(self):
            self.list_calls = 0

        def list_managed_databases(self):
            self.list_calls += 1
            return [_db("db_1", "dlt")]

        def list_managed_tables(self, database, *, schema):
            assert database == "db_1"  # addressed by id after the first resolve
            return []

        def close(self):
            return None

    rt = FakeRuntime()
    cache = SimpleNamespace()
    client = _client(rt, cache=cache)
    client.resolve_managed_database("dlt")
    client.list_managed_tables("dlt", schema="public")

    client2 = _client(rt, cache=cache)
    assert client2.resolve_managed_database("dlt").id == "db_1"

    assert rt.list_calls == 1
    assert cache._hotdata_resolved_db.id == "db_1"
    client.close()
    client2.close()


# --- upload / load: addressed by id ---------------------------------------


def test_upload_and_load_managed_table_addresses_by_id() -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.upload_calls = 0
            self.load_database = None

        def list_managed_databases(self):
            return [_db("db_dlt", "dlt")]

        def upload_parquet(self, path: str) -> str:
            self.upload_calls += 1
            assert path.endswith(".parquet")
            return "upload_1"

        def load_managed_table(
            self, database, table, *, schema, upload_id, mode="replace", key=None
        ):
            self.load_database = database
            return SimpleNamespace(
                connection_id="conn_1",
                schema_name=schema,
                table_name=table,
                row_count=1,
                full_name=f"{database}.{schema}.{table}",
            )

        def close(self) -> None:
            return None

    rt = FakeRuntime()
    client = _client(rt)

    upload_id = client.upload_parquet("/tmp/batch.parquet")
    loaded = client.load_managed_table("dlt", "orders", schema="public", upload_id=upload_id)

    assert upload_id == "upload_1"
    # the load is addressed by the resolved id, not the display name
    assert rt.load_database == "db_dlt"
    assert loaded.full_name == "db_dlt.public.orders"
    assert rt.upload_calls == 1
    client.close()


# --- fetch_table / execute_sql: carry the database scope ------------------


def _patch_query_apis(monkeypatch, arrow_table, *, arrow_in_hotdata_client: bool) -> dict:
    """Patch the query/result APIs so no real HTTP happens.

    ``fetch_table`` resolves ArrowResultsApi from hotdata_framework.managed_client;
    ``execute_sql`` resolves it from hotdata_dlt_destination.hotdata_client.
    Returns a recorder of the ``x_database_id`` scopes each read carried.
    """
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


def test_fetch_table_carries_database_scope(monkeypatch) -> None:
    # Hosted result endpoints reject requests without the database scope;
    # fetch_table (merge / state-sync read-back) must carry it on the result
    # poll and the Arrow fetch, addressing by the resolved id.
    import pyarrow as pa

    scopes = _patch_query_apis(monkeypatch, pa.table({"id": [1]}), arrow_in_hotdata_client=False)

    class FakeRuntime:
        api = None

        def list_managed_databases(self):
            return [_db("db_42", "dlt")]

        def list_managed_tables(self, database, *, schema):
            assert database == "db_42"
            return [SimpleNamespace(table="orders", synced=True)]

        def close(self):
            return None

    client = _client(FakeRuntime())
    table = client.fetch_table(database="dlt", schema="public", table="orders")
    assert table is not None and table.num_rows == 1
    assert scopes["result"] == ["db_42"]
    assert scopes["arrow"] == ["db_42"]
    client.close()


def test_execute_sql_carries_database_scope(monkeypatch) -> None:
    import pyarrow as pa

    scopes = _patch_query_apis(monkeypatch, pa.table({"id": [1, 2]}), arrow_in_hotdata_client=True)

    class FakeRuntime:
        api = None

        def list_managed_databases(self):
            return [_db("db_99", "dlt")]

        def close(self):
            return None

    client = _client(FakeRuntime())
    table = client.execute_sql('SELECT * FROM "default"."public"."spans"', database="dlt")
    assert table.num_rows == 2
    assert scopes["result"] == ["db_99"]
    assert scopes["arrow"] == ["db_99"]
    client.close()


def test_fetch_table_rows_skips_unsynced_tables() -> None:
    class FakeRuntime:
        def list_managed_databases(self):
            return [_db("db_1", "dlt")]

        def list_managed_tables(self, database, *, schema):
            assert database == "db_1"
            return [SimpleNamespace(table="orders", synced=False)]

        def close(self) -> None:
            return None

    client = _client(FakeRuntime())
    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == []
    client.close()


def test_fetch_table_rows_returns_empty_when_table_missing() -> None:
    class FakeRuntime:
        def list_managed_databases(self):
            return [_db("db_1", "dlt")]

        def list_managed_tables(self, database, *, schema):
            return []

        def close(self) -> None:
            return None

    client = _client(FakeRuntime())
    rows = client.fetch_table_rows(database="dlt", schema="public", table="orders")
    assert rows == []
    client.close()


# --- ensure_managed_database / drop_managed_database (schema evolution) ---


class _EvoRuntime:
    """Fake runtime tracking the managed-database lifecycle calls."""

    def __init__(self, existing_db, existing_tables) -> None:
        self._existing_db = existing_db  # _db(...) or None
        self._existing_tables = list(existing_tables)
        self.created: list[tuple] = []
        self.deleted: list[str] = []
        self.added: list[tuple] = []
        self.uploaded: list[str] = []
        self.loaded: list[tuple] = []

    def list_managed_databases(self):
        return [self._existing_db] if self._existing_db is not None else []

    def list_managed_tables(self, database, *, schema):
        return [SimpleNamespace(table=t) for t in self._existing_tables]

    def add_managed_table(self, database, table, *, schema, key=None):
        self.added.append((database, table, schema))
        self._existing_tables.append(table)
        return SimpleNamespace(table=table, schema=schema)

    def create_managed_database(self, *, description, schema, tables, keys=None):
        self.created.append((description, schema, list(tables)))
        return _db("new_db", description)

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


def test_ensure_creates_when_missing() -> None:
    rt = _EvoRuntime(existing_db=None, existing_tables=[])
    client = _client(rt)
    client.ensure_managed_database("db", schema="public", tables=["orders"], create_if_missing=True)
    assert rt.created == [("db", "public", ["orders"])]
    assert rt.deleted == []
    client.close()


def test_ensure_raises_when_missing_and_no_create() -> None:
    rt = _EvoRuntime(existing_db=None, existing_tables=[])
    client = _client(rt)
    with pytest.raises(KeyError):
        client.ensure_managed_database(
            "db", schema="public", tables=["orders"], create_if_missing=False
        )
    client.close()


def test_ensure_noop_when_all_tables_present() -> None:
    rt = _EvoRuntime(existing_db=_db("db_1", "db"), existing_tables=["orders", "customers"])
    client = _client(rt)
    client.ensure_managed_database("db", schema="public", tables=["orders"], create_if_missing=True)
    assert rt.deleted == []
    assert rt.created == []
    assert rt.added == []
    client.close()


def test_ensure_adds_missing_table_without_recreate() -> None:
    rt = _EvoRuntime(existing_db=_db("db_1", "db"), existing_tables=["orders"])
    client = _client(rt)

    client.ensure_managed_database(
        "db", schema="public", tables=["orders", "customers"], create_if_missing=True
    )

    # The missing table is declared in place, addressed by the resolved id; the
    # database is never deleted or recreated and no data is moved.
    assert rt.added == [("db_1", "customers", "public")]
    assert rt.deleted == []
    assert rt.created == []
    assert rt.uploaded == []
    assert rt.loaded == []
    client.close()


def test_ensure_raises_on_ambiguous_name() -> None:
    class _Ambiguous(_EvoRuntime):
        def list_managed_databases(self):
            return [_db("db_1", "db"), _db("db_2", "db")]

    rt = _Ambiguous(existing_db=_db("db_1", "db"), existing_tables=[])
    client = _client(rt)
    with pytest.raises(HotdataTerminalError, match="ambiguous"):
        client.ensure_managed_database(
            "db", schema="public", tables=["orders"], create_if_missing=True
        )
    # never created a duplicate on the ambiguity
    assert rt.created == []
    client.close()


def test_drop_managed_database_deletes_by_id_when_present() -> None:
    rt = _EvoRuntime(existing_db=_db("db_1", "db"), existing_tables=[])
    client = _client(rt)
    client.drop_managed_database("db")
    assert rt.deleted == ["db_1"]
    client.close()


def test_drop_managed_database_noop_when_absent() -> None:
    rt = _EvoRuntime(existing_db=None, existing_tables=[])
    client = _client(rt)
    client.drop_managed_database("db")
    assert rt.deleted == []
    client.close()


def test_drop_clears_run_cache() -> None:
    rt = _EvoRuntime(existing_db=_db("db_1", "db"), existing_tables=[])
    cache = SimpleNamespace()
    client = _client(rt, cache=cache)
    client.resolve_managed_database("db")
    assert cache._hotdata_resolved_db.id == "db_1"
    client.drop_managed_database("db")
    assert cache._hotdata_resolved_db is None
    client.close()
