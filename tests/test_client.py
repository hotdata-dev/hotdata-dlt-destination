from types import SimpleNamespace

import pytest
from hotdata.exceptions import ApiException, ForbiddenException

from hotdata_dlt_destination.hotdata_client import HotdataClient


def _db(db_id: str, name: str = "dlt", conn: str = "conn") -> SimpleNamespace:
    return SimpleNamespace(id=db_id, description=name, default_connection_id=conn)


def _cfg(*, database_id=None, database_name="dlt") -> SimpleNamespace:
    # Stand-in for the shared HotdataClientConfiguration the run binds.
    return SimpleNamespace(database_id=database_id, database_name=database_name)


def _client(runtime, *, config=None) -> HotdataClient:
    client = HotdataClient(
        api_key="k",
        workspace_id="ws",
        api_base_url="https://api.hotdata.dev",
        max_retries=1,
        retry_backoff_seconds=0.0,
    )
    client._runtime = runtime
    if config is not None:
        client.bind_run_cache(config)
    return client


def _install_get_database(monkeypatch, registry: dict) -> None:
    """Patch the id lookup (GET /databases/{id}); unknown ids 404."""

    class FakeDatabasesApi:
        def __init__(self, api):
            pass

        def get_database(self, database_id):
            if database_id not in registry:
                raise ApiException(status=404, reason="not found")
            return registry[database_id]

    monkeypatch.setattr("hotdata_dlt_destination.hotdata_client.DatabasesApi", FakeDatabasesApi)
    monkeypatch.setattr(
        "hotdata_dlt_destination.hotdata_client.managed_database_from_detail", lambda d: d
    )


class _Runtime:
    """Fake runtime tracking managed-database lifecycle calls (no name lookups)."""

    api = None

    def __init__(self, existing_tables=()) -> None:
        self._existing_tables = list(existing_tables)
        self.created: list[tuple] = []
        self.deleted: list[str] = []
        self.added: list[tuple] = []
        self.uploaded: list[str] = []
        self.loaded: list[tuple] = []
        self.table_lists = 0

    def list_managed_tables(self, database, *, schema):
        self.table_lists += 1
        return [SimpleNamespace(table=t, synced=True) for t in self._existing_tables]

    def add_managed_table(self, database, table, *, schema, key=None):
        self.added.append((getattr(database, "id", database), table, schema))
        self._existing_tables.append(table)
        return SimpleNamespace(table=table, schema=schema)

    def create_managed_database(self, *, description, schema, tables, keys=None):
        self.created.append((description, schema, list(tables)))
        return _db("new_db", description)

    def delete_managed_database(self, db):
        self.deleted.append(getattr(db, "id", db))

    def upload_parquet(self, path):
        self.uploaded.append(path)
        return "up_1"

    def load_managed_table(self, database, table, *, schema, upload_id, mode="replace", key=None):
        db_id = getattr(database, "id", database)
        self.loaded.append((db_id, table, schema, upload_id))
        return SimpleNamespace(
            connection_id="c",
            schema_name=schema,
            table_name=table,
            row_count=1,
            full_name=f"{db_id}.{schema}.{table}",
        )

    def close(self):
        return None


# --- create on first run (no database_id) ---------------------------------


def test_create_when_no_id_configured() -> None:
    rt = _Runtime()
    client = _client(rt, config=_cfg(database_id=None, database_name="sales"))
    db = client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=True)
    # created, labelled by database_name; no read/list happened (nothing to reconcile)
    assert rt.created == [("sales", "public", ["orders"])]
    assert db.id == "new_db"
    assert rt.table_lists == 0
    client.close()


def test_create_logs_new_id(monkeypatch) -> None:
    messages: list[str] = []
    monkeypatch.setattr(
        "hotdata_dlt_destination.hotdata_client.logger.warning",
        lambda msg, *args: messages.append(msg % args if args else msg),
    )
    rt = _Runtime()
    client = _client(rt, config=_cfg(database_name="sales"))
    client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=True)
    # the id is surfaced for the user to pin via database_id
    assert any("new_db" in m and "database_id" in m for m in messages)
    client.close()


def test_no_id_and_no_create_raises_keyerror() -> None:
    # is_storage_initialized() path: probe with create disabled and no id -> "not there".
    rt = _Runtime()
    client = _client(rt, config=_cfg(database_id=None))
    with pytest.raises(KeyError):
        client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=False)
    assert rt.created == []
    client.close()


# --- bind an existing database by id --------------------------------------


def test_bind_existing_by_id(monkeypatch) -> None:
    _install_get_database(monkeypatch, {"db_1": _db("db_1", "sales")})
    rt = _Runtime(existing_tables=["orders", "customers"])
    client = _client(rt, config=_cfg(database_id="db_1"))
    db = client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=True)
    # bound by id; not recreated
    assert db.id == "db_1"
    assert rt.created == []
    client.close()


def test_bind_missing_id_raises_keyerror(monkeypatch) -> None:
    _install_get_database(monkeypatch, {})  # id does not exist -> 404
    rt = _Runtime()
    client = _client(rt, config=_cfg(database_id="db_missing"))
    with pytest.raises(KeyError):
        client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=False)
    assert rt.created == []
    client.close()


def test_bind_by_id_evolves_schema_in_place(monkeypatch) -> None:
    _install_get_database(monkeypatch, {"db_1": _db("db_1", "sales")})
    rt = _Runtime(existing_tables=["orders"])
    client = _client(rt, config=_cfg(database_id="db_1"))
    client.ensure_managed_database(
        schema="public", tables=["orders", "customers"], create_if_missing=True
    )
    # the missing table is declared in place, addressed by the resolved id
    assert rt.added == [("db_1", "customers", "public")]
    assert rt.created == []
    assert rt.deleted == []
    client.close()


# --- resolve-once / run cache (populated by id or create, never by name) --


def test_resolves_once_and_reuses_run_cache(monkeypatch) -> None:
    reads = {"n": 0}

    class FakeDatabasesApi:
        def __init__(self, api):
            pass

        def get_database(self, database_id):
            reads["n"] += 1
            return _db("db_1", "sales")

    monkeypatch.setattr("hotdata_dlt_destination.hotdata_client.DatabasesApi", FakeDatabasesApi)
    monkeypatch.setattr(
        "hotdata_dlt_destination.hotdata_client.managed_database_from_detail", lambda d: d
    )

    cfg = _cfg(database_id="db_1")
    rt = _Runtime(existing_tables=["orders"])
    client = _client(rt, config=cfg)
    client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=True)
    # a second client sharing the run config reuses the cached record
    client2 = _client(_Runtime(existing_tables=["orders"]), config=cfg)
    assert client2.resolved_database_id() == "db_1"
    assert reads["n"] == 1  # bound exactly once for the run
    assert cfg._hotdata_db.id == "db_1"
    client.close()
    client2.close()


# --- operations address the resolved record -------------------------------


def test_load_addresses_by_resolved_record(monkeypatch) -> None:
    _install_get_database(monkeypatch, {"db_dlt": _db("db_dlt", "dlt")})
    rt = _Runtime()
    client = _client(rt, config=_cfg(database_id="db_dlt"))
    upload_id = client.upload_parquet("/tmp/batch.parquet")
    loaded = client.load_managed_table("orders", schema="public", upload_id=upload_id)
    assert upload_id == "up_1"
    assert rt.loaded == [("db_dlt", "orders", "public", "up_1")]
    assert loaded.full_name == "db_dlt.public.orders"
    client.close()


def test_list_managed_tables_by_id(monkeypatch) -> None:
    _install_get_database(monkeypatch, {"db_1": _db("db_1")})
    rt = _Runtime(existing_tables=["orders"])
    client = _client(rt, config=_cfg(database_id="db_1"))
    tables = client.list_managed_tables(schema="public")
    assert [t.table for t in tables] == ["orders"]
    client.close()


def _patch_query_apis(monkeypatch, arrow_table, *, arrow_in_hotdata_client: bool) -> dict:
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


def test_execute_sql_carries_database_scope(monkeypatch) -> None:
    import pyarrow as pa

    _install_get_database(monkeypatch, {"db_99": _db("db_99")})
    scopes = _patch_query_apis(monkeypatch, pa.table({"id": [1, 2]}), arrow_in_hotdata_client=True)

    client = _client(_Runtime(), config=_cfg(database_id="db_99"))
    table = client.execute_sql('SELECT * FROM "default"."public"."spans"')
    assert table.num_rows == 2
    assert scopes["result"] == ["db_99"]
    assert scopes["arrow"] == ["db_99"]
    client.close()


def test_fetch_table_carries_database_scope(monkeypatch) -> None:
    import pyarrow as pa

    _install_get_database(monkeypatch, {"db_42": _db("db_42")})
    scopes = _patch_query_apis(monkeypatch, pa.table({"id": [1]}), arrow_in_hotdata_client=False)
    rt = _Runtime(existing_tables=["orders"])
    client = _client(rt, config=_cfg(database_id="db_42"))
    table = client.fetch_table(schema="public", table="orders")
    assert table is not None and table.num_rows == 1
    assert scopes["result"] == ["db_42"]
    assert scopes["arrow"] == ["db_42"]
    client.close()


def test_fetch_table_rows_skips_unsynced_tables(monkeypatch) -> None:
    _install_get_database(monkeypatch, {"db_1": _db("db_1")})

    class _Unsynced(_Runtime):
        def list_managed_tables(self, database, *, schema):
            return [SimpleNamespace(table="orders", synced=False)]

    client = _client(_Unsynced(), config=_cfg(database_id="db_1"))
    assert client.fetch_table_rows(schema="public", table="orders") == []
    client.close()


# --- drop -----------------------------------------------------------------


def test_drop_deletes_by_id(monkeypatch) -> None:
    _install_get_database(monkeypatch, {"db_1": _db("db_1")})
    rt = _Runtime()
    cfg = _cfg(database_id="db_1")
    client = _client(rt, config=cfg)
    client.drop_managed_database()
    assert rt.deleted == ["db_1"]
    assert cfg._hotdata_db is None
    client.close()


def test_drop_noop_without_id() -> None:
    rt = _Runtime()
    client = _client(rt, config=_cfg(database_id=None))
    client.drop_managed_database()
    assert rt.deleted == []
    client.close()


# --- #55: create/upload/query-scoped keys (forbidden reads) bootstrap ------
# Under id-first there is no id to look up on a first run, so the create path
# never issues a read the key isn't allowed to make -- create just succeeds.


class _CreateScopedRuntime(_Runtime):
    """A create-scoped key: any read (get_database / list) is forbidden."""

    def list_managed_tables(self, database, *, schema):
        raise ForbiddenException(status=403, reason="ACCESS_DENIED")


def test_create_scoped_key_bootstraps_without_read(monkeypatch) -> None:
    # get_database must never be called on the create path.
    called = {"get": 0}

    class FakeDatabasesApi:
        def __init__(self, api):
            pass

        def get_database(self, database_id):
            called["get"] += 1
            raise ForbiddenException(status=403, reason="ACCESS_DENIED")

    monkeypatch.setattr("hotdata_dlt_destination.hotdata_client.DatabasesApi", FakeDatabasesApi)

    rt = _CreateScopedRuntime()
    cfg = _cfg(database_id=None, database_name="dlt")
    client = _client(rt, config=cfg)
    db = client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=True)
    assert rt.created == [("dlt", "public", ["orders"])]
    assert db.id == "new_db"
    assert called["get"] == 0  # never attempted a forbidden read
    assert cfg._hotdata_db.id == "new_db"
    client.close()


def test_create_scoped_load_reuses_cache_without_read(monkeypatch) -> None:
    class FakeDatabasesApi:
        def __init__(self, api):
            pass

        def get_database(self, database_id):
            raise ForbiddenException(status=403, reason="ACCESS_DENIED")

    monkeypatch.setattr("hotdata_dlt_destination.hotdata_client.DatabasesApi", FakeDatabasesApi)

    rt = _CreateScopedRuntime()
    cfg = _cfg(database_id=None, database_name="dlt")
    client = _client(rt, config=cfg)
    client.ensure_managed_database(schema="public", tables=["orders"], create_if_missing=True)
    # a load in the same run resolves from cache and hands the record straight through
    client.load_managed_table("orders", schema="public", upload_id="u1")
    assert rt.loaded == [("new_db", "orders", "public", "u1")]
    client.close()
