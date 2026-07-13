from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common.schema import Schema
from dlt.common.schema.typing import (
    LOADS_TABLE_NAME,
    PIPELINE_STATE_TABLE_NAME,
    VERSION_TABLE_NAME,
)

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination import job_client as jc
from hotdata_dlt_destination.configuration import HotdataClientConfiguration, HotdataCredentials
from hotdata_dlt_destination.job_client import HotdataJobClient, HotdataLoadJob, _declared_tables


def _make_fake_api_cls(store: dict[str, pa.Table]):
    """A fake managed-database client backed by an in-memory ``store`` dict.

    Each ``_hotdata_api`` context constructs a fresh instance, so state must
    live in the shared ``store`` rather than on the instance.
    """

    class FakeApi:
        def __init__(self, **_kwargs: object) -> None:
            self._pending: pa.Table | None = None

        def ensure_managed_database(self, name, *, schema, tables, keys=None, create_if_missing):
            return SimpleNamespace(id="db_1")

        def fetch_table(self, *, database, schema, table):
            return store.get(table)

        def upload_parquet(self, path: str) -> str:
            self._pending = pq.read_table(path)
            return "upload_1"

        def load_managed_table(self, database, table, *, schema, upload_id, mode="replace"):
            assert self._pending is not None
            store[table] = self._pending
            return SimpleNamespace(full_name=f"{database}.{schema}.{table}")

        def close(self) -> None:
            return None

    return FakeApi


def _config(**overrides) -> HotdataClientConfiguration:
    base = {
        "credentials": HotdataCredentials(api_key="k", workspace_id="ws"),
        "database_name": "dlt",
        "schema": "public",
        "write_disposition": "append",
        "create_database_if_missing": True,
        "max_retries": 1,
        "retry_backoff_seconds": 0.0,
    }
    base.update(overrides)
    return HotdataClientConfiguration(**base)


# --- declared tables ---


def test_declared_tables_includes_internal_and_target() -> None:
    from hotdata_dlt_destination.contracts import TableContract

    contract = TableContract.from_table_schema(
        {"name": "orders"}, database_name="dlt", schema="public"
    )
    tables = _declared_tables(contract=contract, declared_tables=["customers"])
    assert "orders" in tables
    assert "customers" in tables
    assert VERSION_TABLE_NAME in tables
    assert LOADS_TABLE_NAME in tables
    assert PIPELINE_STATE_TABLE_NAME in tables


# --- load job ---


def _write_parquet(tmp_path, rows: list[dict], table: str = "orders") -> str:
    # RunnableLoadJob parses the file name as table_name.file_id.retry_count.file_format
    path = str(tmp_path / f"{table}.abc123.0.parquet")
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_load_job_replace_uploads_incoming(tmp_path, monkeypatch) -> None:
    store: dict[str, pa.Table] = {}
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))

    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}, {"id": 2, "_dlt_id": "b"}])
    job = HotdataLoadJob(path, _config(), {"name": "orders", "write_disposition": "replace"})
    job.run()

    assert store["orders"].to_pylist() == [
        {"id": 1, "_dlt_id": "a"},
        {"id": 2, "_dlt_id": "b"},
    ]


def test_load_job_merge_combines_with_existing(tmp_path, monkeypatch) -> None:
    store: dict[str, pa.Table] = {
        "orders": pa.Table.from_pylist([{"id": 1, "_dlt_id": "a", "v": "old"}])
    }
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))

    path = _write_parquet(
        tmp_path,
        [{"id": 1, "_dlt_id": "a", "v": "new"}, {"id": 2, "_dlt_id": "b", "v": "added"}],
    )
    job = HotdataLoadJob(
        path,
        _config(),
        {"name": "orders", "write_disposition": "merge", "primary_key": ["id"]},
    )
    job.run()

    assert store["orders"].to_pylist() == [
        {"id": 1, "_dlt_id": "a", "v": "new"},
        {"id": 2, "_dlt_id": "b", "v": "added"},
    ]


def _recording_api_cls(calls: dict, reject_mode: str | None = None):
    from hotdata_dlt_destination.errors import HotdataTerminalError

    class RecordingApi:
        def __init__(self, **_kwargs: object) -> None:
            self._pending = None

        def ensure_managed_database(self, name, *, schema, tables, keys=None, create_if_missing):
            calls["keys"] = keys
            return SimpleNamespace(id="db_1")

        def fetch_table(self, *, database, schema, table):
            calls["fetches"] = calls.get("fetches", 0) + 1
            return None

        def upload_parquet(self, path: str) -> str:
            self._pending = pq.read_table(path)
            return "upload_1"

        def load_managed_table(self, database, table, *, schema, upload_id, mode="replace"):
            calls.setdefault("modes", []).append(mode)
            calls["mode"] = mode
            if reject_mode is not None and mode == reject_mode:
                raise HotdataTerminalError(
                    f"table '{schema}.{table}' has no declared key; a key is required for mode={mode}"
                )
            return SimpleNamespace(full_name=f"{database}.{schema}.{table}")

        def close(self) -> None:
            return None

    return RecordingApi


def test_merge_with_key_uses_native_upsert_no_rmw(tmp_path, monkeypatch) -> None:
    # merge + a declared primary key -> native mode=upsert, key declared on
    # ensure, and NO full-table read-modify-write.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    job = HotdataLoadJob(
        path, _config(), {"name": "orders", "write_disposition": "merge", "primary_key": ["id"]}
    )
    job.run()
    assert calls["mode"] == "upsert"
    assert calls.get("fetches", 0) == 0
    assert calls["keys"] == {"orders": ["id"]}


def test_append_uses_native_append_no_rmw(tmp_path, monkeypatch) -> None:
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    job = HotdataLoadJob(path, _config(), {"name": "orders", "write_disposition": "append"})
    job.run()
    assert calls["mode"] == "append"
    assert calls.get("fetches", 0) == 0


def test_keyless_merge_falls_back_to_rmw_replace(tmp_path, monkeypatch) -> None:
    # merge with no resolvable key -> client-side combine, then replace.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    job = HotdataLoadJob(path, _config(), {"name": "orders", "write_disposition": "merge"})
    job.run()
    assert calls["mode"] == "replace"
    assert calls.get("fetches", 0) == 1


def test_insert_only_merge_does_not_native_upsert(tmp_path, monkeypatch) -> None:
    # insert-only is a merge *strategy* (write_disposition is still "merge"); it
    # must NOT take the native upsert path, which would update existing rows.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    job = HotdataLoadJob(
        path,
        _config(),
        {
            "name": "orders",
            "write_disposition": "merge",
            "primary_key": ["id"],
            "x-merge-strategy": "insert-only",
        },
    )
    job.run()
    assert calls["mode"] == "replace"  # client-side combine, not native upsert
    assert calls.get("fetches", 0) == 1


def test_upsert_falls_back_to_rmw_on_missing_server_key(tmp_path, monkeypatch) -> None:
    # A table created before server-side key support has no declared key and
    # rejects upsert; the connector must fall back to combine + replace, not fail.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls, reject_mode="upsert"))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    job = HotdataLoadJob(
        path, _config(), {"name": "orders", "write_disposition": "merge", "primary_key": ["id"]}
    )
    job.run()  # must not raise
    assert calls["modes"] == ["upsert", "replace"]  # tried native, then fell back
    assert calls.get("fetches", 0) == 1


# --- state sync ---


def _client(store: dict[str, pa.Table], monkeypatch) -> HotdataJobClient:
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))
    schema = Schema("events")
    return HotdataJobClient(schema, _config(), hotdata().capabilities())


def test_complete_load_appends_loads_row(monkeypatch) -> None:
    store: dict[str, pa.Table] = {}
    client = _client(store, monkeypatch)
    client.complete_load("load_1")

    rows = store[LOADS_TABLE_NAME].to_pylist()
    assert len(rows) == 1
    assert rows[0]["load_id"] == "load_1"
    assert rows[0]["status"] == 0


def test_is_storage_initialized_false_when_missing(monkeypatch) -> None:
    class MissingApi:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def ensure_managed_database(self, name, *, schema, tables, keys=None, create_if_missing):
            raise KeyError(name)

        def close(self) -> None:
            return None

    monkeypatch.setattr(jc, "HotdataClient", MissingApi)
    client = HotdataJobClient(Schema("events"), _config(), hotdata().capabilities())
    assert client.is_storage_initialized() is False


def test_get_stored_state_filters_by_completed_loads(monkeypatch) -> None:
    now = datetime.now(UTC)
    store: dict[str, pa.Table] = {
        LOADS_TABLE_NAME: pa.Table.from_pylist(
            [{"load_id": "load_1", "status": 0}, {"load_id": "load_2", "status": 0}]
        ),
        PIPELINE_STATE_TABLE_NAME: pa.Table.from_pylist(
            [
                {
                    "version": 1,
                    "engine_version": 1,
                    "pipeline_name": "events",
                    "state": "s1",
                    "created_at": now,
                    "version_hash": "h1",
                    "_dlt_load_id": "load_1",
                },
                {
                    "version": 2,
                    "engine_version": 1,
                    "pipeline_name": "events",
                    "state": "s2",
                    "created_at": now,
                    "version_hash": "h2",
                    "_dlt_load_id": "load_2",
                },
            ]
        ),
    }
    client = _client(store, monkeypatch)
    info = client.get_stored_state("events")
    assert info is not None
    # load_2 is the most recent completed load
    assert info.state == "s2"
    assert info._dlt_load_id == "load_2"


def test_get_stored_state_none_when_no_completed_loads(monkeypatch) -> None:
    store: dict[str, pa.Table] = {}
    client = _client(store, monkeypatch)
    assert client.get_stored_state("events") is None


def test_get_stored_schema_returns_latest(monkeypatch) -> None:
    older = datetime(2024, 1, 1, tzinfo=UTC)
    newer = datetime(2024, 6, 1, tzinfo=UTC)
    store: dict[str, pa.Table] = {
        VERSION_TABLE_NAME: pa.Table.from_pylist(
            [
                {
                    "version": 1,
                    "engine_version": 1,
                    "inserted_at": older,
                    "schema_name": "events",
                    "version_hash": "old",
                    "schema": "{}",
                },
                {
                    "version": 2,
                    "engine_version": 1,
                    "inserted_at": newer,
                    "schema_name": "events",
                    "version_hash": "new",
                    "schema": "{}",
                },
            ]
        )
    }
    client = _client(store, monkeypatch)
    info = client.get_stored_schema()
    assert info is not None
    assert info.version_hash == "new"
