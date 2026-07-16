from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException
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
            # Mode-faithful, like the server: append accumulates, replace overwrites.
            existing = store.get(table)
            if mode == "append" and existing is not None:
                store[table] = pa.concat_tables(
                    [existing, self._pending], promote_options="permissive"
                )
            else:
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


def _write_parquet(
    tmp_path, rows: list[dict], table: str = "orders", file_id: str = "abc123"
) -> str:
    # RunnableLoadJob parses the file name as table_name.file_id.retry_count.file_format
    path = str(tmp_path / f"{table}.{file_id}.0.parquet")
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


def test_replace_multi_file_keeps_all_files(tmp_path, monkeypatch) -> None:
    # A table split across multiple parquet files (DATA_WRITER__FILE_MAX_BYTES)
    # runs one load job per file. Replace semantics come from the package-init
    # truncate; each file job must APPEND, or every file wipes the previous one
    # and only the last file's rows survive.
    store: dict[str, pa.Table] = {}
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))

    for file_id, rows in (
        ("aaa", [{"id": 1, "_dlt_id": "a"}]),
        ("bbb", [{"id": 2, "_dlt_id": "b"}]),
        ("ccc", [{"id": 3, "_dlt_id": "c"}]),
    ):
        path = _write_parquet(tmp_path, rows, file_id=file_id)
        HotdataLoadJob(path, _config(), {"name": "orders", "write_disposition": "replace"}).run()

    assert sorted(r["id"] for r in store["orders"].to_pylist()) == [1, 2, 3]


def test_replace_file_jobs_load_with_append_mode(tmp_path, monkeypatch) -> None:
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    job = HotdataLoadJob(path, _config(), {"name": "orders", "write_disposition": "replace"})
    job.run()
    assert calls["mode"] == "append"
    assert calls.get("fetches", 0) == 0


def test_initialize_storage_truncates_replace_tables(monkeypatch) -> None:
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    schema = Schema("events")
    schema.update_table(
        {
            "name": "orders",
            "write_disposition": "replace",
            "columns": {"id": {"name": "id", "data_type": "bigint"}},
        }
    )
    client = HotdataJobClient(schema, _config(), hotdata().capabilities())

    client.initialize_storage(truncate_tables=["orders", "_dlt_loads"])

    # orders emptied via a zero-row replace load (the API's only truncate — the
    # delete-table endpoint tombstones); internal dlt tables never truncated.
    assert calls.get("loads") == [("orders", "replace", 0)]


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

        def upload_parquet(self, path: str) -> str:
            self._pending = pq.read_table(path)
            return "upload_1"

        def load_managed_table(self, database, table, *, schema, upload_id, mode="replace"):
            calls.setdefault("modes", []).append(mode)
            calls["mode"] = mode
            pending = self._pending.to_pylist() if self._pending is not None else None
            calls.setdefault("uploads", []).append((mode, pending))
            calls.setdefault("loads", []).append(
                (table, mode, self._pending.num_rows if self._pending is not None else None)
            )
            if reject_mode is not None and mode == reject_mode:
                raise HotdataTerminalError(f"{table}: no declared key; required for mode={mode}")
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


def test_merge_hard_delete_splits_upsert_and_delete(tmp_path, monkeypatch) -> None:
    # A merge batch carrying a hard_delete column -> upsert the live rows, delete
    # the flagged rows by key. The delete upload carries key columns only.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(
        tmp_path,
        [
            {"id": 1, "v": "a", "_dlt_id": "x", "deleted": False},
            {"id": 2, "v": "b", "_dlt_id": "y", "deleted": True},
        ],
    )
    table = {
        "name": "orders",
        "write_disposition": "merge",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "deleted": {"name": "deleted", "data_type": "bool", "hard_delete": True},
        },
    }
    HotdataLoadJob(path, _config(), table).run()

    assert calls["modes"] == ["upsert", "delete"]
    uploads = dict(calls["uploads"])
    assert [r["id"] for r in uploads["upsert"]] == [1]
    assert uploads["delete"] == [{"id": 2}]  # only the flagged key, key columns only
    assert calls.get("fetches", 0) == 0  # native path, no read-modify-write


def _hd_table(*, strategy: str | None = None, keyed: bool = True, flag: str = "deleted") -> dict:
    cols: dict = {flag: {"name": flag, "data_type": "bool", "hard_delete": True}}
    if keyed:
        cols["id"] = {"name": "id", "data_type": "bigint", "primary_key": True}
    table: dict = {"name": "orders", "write_disposition": "merge", "columns": cols}
    if strategy:
        table["x-merge-strategy"] = strategy
    return table


def test_insert_only_hard_delete_excludes_flagged_rows(tmp_path, monkeypatch) -> None:
    # insert-only + hard_delete: a flagged row must NOT be inserted as live data.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(
        tmp_path,
        [{"id": 1, "_dlt_id": "x", "deleted": False}, {"id": 2, "_dlt_id": "y", "deleted": True}],
    )
    HotdataLoadJob(path, _config(), _hd_table(strategy="insert-only")).run()
    assert calls["modes"] == ["replace"]  # insert-only -> client combine + replace
    assert [r["id"] for r in dict(calls["uploads"])["replace"]] == [1]  # id=2 not written


def test_keyless_merge_hard_delete_excludes_flagged_rows(tmp_path, monkeypatch) -> None:
    # merge with no primary_key + hard_delete: a flagged row must not land as live data.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(
        tmp_path,
        [{"id": 1, "_dlt_id": "x", "deleted": False}, {"id": 2, "_dlt_id": "y", "deleted": True}],
    )
    HotdataLoadJob(path, _config(), _hd_table(keyed=False)).run()
    assert calls["modes"] == ["replace"]
    assert [r["id"] for r in dict(calls["uploads"])["replace"]] == [1]  # id=2 not written


def test_merge_hard_delete_all_flagged_routes_to_replace(tmp_path, monkeypatch) -> None:
    # Every row flagged -> no upsert to initialise the table, so route through
    # combine + replace (a native delete could 500 on a never-loaded table).
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(
        tmp_path,
        [{"id": 1, "_dlt_id": "x", "deleted": True}, {"id": 2, "_dlt_id": "y", "deleted": True}],
    )
    HotdataLoadJob(path, _config(), _hd_table()).run()
    assert calls["modes"] == ["replace"]  # not a native delete against a never-loaded table
    assert dict(calls["uploads"])["replace"] == []  # all flagged -> empty result
    assert calls.get("fetches", 0) == 1  # combine fetched existing (None here)


def test_insert_only_hard_delete_into_populated_excludes_flagged(tmp_path, monkeypatch) -> None:
    # insert-only into a NON-empty table exercises combine_tables' insert-only
    # branch: a flagged row must not be inserted; existing rows survive.
    store = {"orders": pa.Table.from_pylist([{"id": 100, "_dlt_id": "seed", "deleted": False}])}
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))
    path = _write_parquet(
        tmp_path,
        [{"id": 3, "_dlt_id": "n", "deleted": False}, {"id": 4, "_dlt_id": "m", "deleted": True}],
    )
    HotdataLoadJob(path, _config(), _hd_table(strategy="insert-only")).run()
    assert sorted(r["id"] for r in store["orders"].to_pylist()) == [3, 100]  # 4 flagged, dropped


def test_keyless_merge_hard_delete_into_populated_drops_flagged(tmp_path, monkeypatch) -> None:
    # keyless merge into a NON-empty table exercises combine_tables' merge branch
    # hard-delete handling: flagged incoming dropped; existing rows survive.
    store = {"orders": pa.Table.from_pylist([{"id": 100, "_dlt_id": "seed", "deleted": False}])}
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))
    path = _write_parquet(
        tmp_path,
        [{"id": 3, "_dlt_id": "n", "deleted": False}, {"id": 4, "_dlt_id": "m", "deleted": True}],
    )
    HotdataLoadJob(path, _config(), _hd_table(keyed=False)).run()
    assert sorted(r["id"] for r in store["orders"].to_pylist()) == [3, 100]  # 4 flagged, dropped


def test_hard_delete_column_in_primary_key_is_terminal(tmp_path, monkeypatch) -> None:
    # A key column doubling as the delete flag would corrupt the delete -> fail fast.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(tmp_path, [{"id": 1, "_dlt_id": "a"}])
    table = {
        "name": "orders",
        "write_disposition": "merge",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True, "hard_delete": True}
        },
    }
    with pytest.raises(DestinationTerminalException):
        HotdataLoadJob(path, _config(), table).run()


def test_merge_hard_delete_non_bool_deletes_on_not_null(tmp_path, monkeypatch) -> None:
    # A non-bool hard_delete column (e.g. deleted_at) deletes on NOT NULL.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    path = _write_parquet(
        tmp_path,
        [
            {"id": 1, "_dlt_id": "x", "deleted_at": None},
            {"id": 2, "_dlt_id": "y", "deleted_at": "2026-01-01"},
        ],
    )
    table = {
        "name": "orders",
        "write_disposition": "merge",
        "columns": {
            "id": {"name": "id", "data_type": "bigint", "primary_key": True},
            "deleted_at": {"name": "deleted_at", "data_type": "text", "hard_delete": True},
        },
    }
    HotdataLoadJob(path, _config(), table).run()
    assert calls["modes"] == ["upsert", "delete"]
    uploads = dict(calls["uploads"])
    assert [r["id"] for r in uploads["upsert"]] == [1]  # null deleted_at -> live
    assert [r["id"] for r in uploads["delete"]] == [2]  # non-null deleted_at -> delete


def test_hard_delete_fallback_on_missing_server_key_excludes_flagged(tmp_path, monkeypatch) -> None:
    # Server rejects upsert (no declared key) -> combine + replace fallback that
    # still drops the hard-delete-flagged rows.
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls, reject_mode="upsert"))
    path = _write_parquet(
        tmp_path,
        [{"id": 1, "_dlt_id": "x", "deleted": False}, {"id": 2, "_dlt_id": "y", "deleted": True}],
    )
    HotdataLoadJob(path, _config(), _hd_table()).run()  # must not raise
    assert calls["modes"] == ["upsert", "replace"]  # native upsert rejected -> fallback
    assert [r["id"] for r in dict(calls["uploads"])["replace"]] == [1]  # id=2 not written


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
