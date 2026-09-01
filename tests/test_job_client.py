from __future__ import annotations

import re
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
from hotdata_dlt_destination.errors import HotdataTerminalError
from hotdata_dlt_destination.job_client import HotdataJobClient, HotdataLoadJob, _declared_tables


def _make_fake_api_cls(store: dict[str, pa.Table]):
    """A fake instant-database client backed by an in-memory ``store`` dict.

    Each ``_hotdata_api`` context constructs a fresh instance, so state must
    live in the shared ``store`` rather than on the instance.
    """

    class FakeApi:
        def __init__(self, **_kwargs: object) -> None:
            self._pending: pa.Table | None = None

        def bind_run_cache(self, cache: object) -> None:
            return None

        def ensure_managed_database(
            self, *, schema, tables, keys=None, partition_by=None, sorted_by=None,
            create_if_missing,
        ):
            # Nothing declared here, so nothing is newly created — which is what
            # stops initialize_storage seeding tables that may already hold data.
            self.newly_declared = set()
            return SimpleNamespace(id="db_1")

        def fetch_table(self, *, schema, table):
            return store.get(table)

        def execute_sql(self, sql: str) -> pa.Table:
            """Run the pushdown reads on a real engine, so dialect behaviour matches.

            The internal-table reads carry their predicate in SQL (WHERE / ORDER
            BY / LIMIT / a join), so a store lookup cannot stand in for them.
            """
            from datafusion import SessionContext

            ctx = SessionContext()
            for name, tbl in store.items():
                # An empty pyarrow table yields NO batches, and DataFusion panics
                # registering a zero-batch partition.
                batches = tbl.to_batches() or [
                    pa.RecordBatch.from_pylist([], schema=tbl.schema)
                ]
                ctx.register_record_batches(name, [batches])
            # Strip the "default"."<schema>". qualifier so bare names resolve.
            rewritten = re.sub(r'"default"\."[^"]+"\.(?=")', "", sql)
            try:
                return ctx.sql(rewritten).to_arrow_table()
            except Exception as exc:
                # The real client surfaces query failures as HotdataTerminalError;
                # an unknown table has to look the same here so the production
                # "nothing stored yet" path is what the tests exercise.
                raise HotdataTerminalError(str(exc)) from exc

        def upload_parquet(self, path: str) -> str:
            self._pending = pq.read_table(path)
            return "upload_1"

        def load_managed_table(self, table, *, schema, upload_id, mode="replace", key=None):
            assert self._pending is not None
            # Mode-faithful, like the server: append accumulates, replace
            # overwrites, delete removes the incoming keys.
            existing = store.get(table)
            if mode == "append" and existing is not None:
                store[table] = pa.concat_tables(
                    [existing, self._pending], promote_options="permissive"
                )
            elif mode == "delete":
                if existing is not None:
                    assert key, "delete needs a key"
                    doomed = {
                        tuple(r[k] for k in key) for r in self._pending.to_pylist()
                    }
                    kept = [
                        r for r in existing.to_pylist()
                        if tuple(r[k] for k in key) not in doomed
                    ]
                    store[table] = pa.Table.from_pylist(kept, schema=existing.schema)
            else:
                store[table] = self._pending
            return SimpleNamespace(full_name=f"db_1.{schema}.{table}")

        def close(self) -> None:
            return None

    return FakeApi


def _config(**overrides) -> HotdataClientConfiguration:
    base = {
        "credentials": HotdataCredentials(api_key="k"),
        "workspace_id": "ws",
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

        def bind_run_cache(self, cache: object) -> None:
            return None

        def ensure_managed_database(
            self, *, schema, tables, keys=None, partition_by=None, sorted_by=None,
            create_if_missing,
        ):
            calls["keys"] = keys
            calls["partition_by"] = partition_by
            calls["sorted_by"] = sorted_by
            self.newly_declared = set()
            return SimpleNamespace(id="db_1")

        def fetch_table(self, *, schema, table):
            calls["fetches"] = calls.get("fetches", 0) + 1

        def upload_parquet(self, path: str) -> str:
            self._pending = pq.read_table(path)
            return "upload_1"

        def load_managed_table(self, table, *, schema, upload_id, mode="replace", key=None):
            calls.setdefault("modes", []).append(mode)
            calls["mode"] = mode
            calls["load_key"] = key
            calls.setdefault("load_keys", []).append(key)
            pending = self._pending.to_pylist() if self._pending is not None else None
            calls.setdefault("uploads", []).append((mode, pending))
            calls.setdefault("loads", []).append(
                (table, mode, self._pending.num_rows if self._pending is not None else None)
            )
            if reject_mode is not None and mode == reject_mode:
                raise HotdataTerminalError(f"{table}: no declared key; required for mode={mode}")
            return SimpleNamespace(full_name=f"db_1.{schema}.{table}")

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
    # The per-load key is sent on the upsert (matches even without a declared key).
    assert calls["load_key"] == ["id"]


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


def _client(store: dict[str, pa.Table], monkeypatch, **config_overrides) -> HotdataJobClient:
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls(store))
    schema = Schema("events")
    return HotdataJobClient(schema, _config(**config_overrides), hotdata().capabilities())


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

        def bind_run_cache(self, cache: object) -> None:
            return None

        def ensure_managed_database(self, *, schema, tables, keys=None, create_if_missing):
            raise KeyError("db_1")

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


# --- storage layout -----------------------------------------------------------
#
# Two things can go wrong here and only one is loud. A hint that never reaches
# the API leaves a permanently unpartitioned table; a seed that fires on the
# wrong table EMPTIES it, because the seed is a zero-row replace load.


def _layout_schema(disposition: str) -> Schema:
    schema = Schema("events")
    schema.update_table(
        {
            "name": "orders",
            "write_disposition": disposition,
            "columns": {
                "id": {"name": "id", "data_type": "bigint"},
                "event_date": {"name": "event_date", "data_type": "date", "partition": True},
                "event_time": {"name": "event_time", "data_type": "timestamp", "sort": True},
            },
        }
    )
    return schema


def test_layout_reaches_the_api_on_declaration(monkeypatch) -> None:
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    client = HotdataJobClient(_layout_schema("append"), _config(), hotdata().capabilities())

    client.initialize_storage(truncate_tables=[])

    parts = calls.get("partition_by") or {}
    sorts = calls.get("sorted_by") or {}
    assert [k.column for k in parts.get("orders", [])] == ["event_date"]
    assert [k.column for k in sorts.get("orders", [])] == ["event_time"]


def test_an_append_table_with_a_layout_is_seeded_when_newly_created(monkeypatch) -> None:
    """The constraint that makes this feature dangerous to get wrong: the FIRST
    load into a layout-declaring table must be `replace`, and our append path
    loads with mode="append". Without this seed the table's first load is refused
    outright, turning a working append pipeline into a hard failure."""
    calls: dict = {}
    api_cls = _recording_api_cls(calls)

    class NewTable(api_cls):  # type: ignore[misc,valid-type]
        def ensure_managed_database(self, **kwargs):
            db = super().ensure_managed_database(**kwargs)
            self.newly_declared = {"orders"}  # this run created it
            return db

    monkeypatch.setattr(jc, "HotdataClient", NewTable)
    client = HotdataJobClient(_layout_schema("append"), _config(), hotdata().capabilities())

    client.initialize_storage(truncate_tables=[])

    assert calls.get("loads") == [("orders", "replace", 0)]


def test_an_existing_table_is_never_seeded(monkeypatch) -> None:
    """The dangerous direction. The seed is a zero-row REPLACE, so firing it on a
    table that already holds data would empty it. Only tables this run created
    may be seeded."""
    calls: dict = {}
    monkeypatch.setattr(jc, "HotdataClient", _recording_api_cls(calls))
    client = HotdataJobClient(_layout_schema("append"), _config(), hotdata().capabilities())

    # The recording double reports newly_declared = set(), i.e. nothing created.
    client.initialize_storage(truncate_tables=[])

    assert calls.get("loads") in (None, []), calls.get("loads")


def test_a_new_table_without_a_layout_is_not_seeded(monkeypatch) -> None:
    """No layout means no first-load-must-be-replace constraint, so seeding it
    would be a pointless extra load."""
    calls: dict = {}
    api_cls = _recording_api_cls(calls)

    class NewTable(api_cls):  # type: ignore[misc,valid-type]
        def ensure_managed_database(self, **kwargs):
            db = super().ensure_managed_database(**kwargs)
            self.newly_declared = {"orders"}
            return db

    monkeypatch.setattr(jc, "HotdataClient", NewTable)
    schema = Schema("events")
    schema.update_table(
        {
            "name": "orders",
            "write_disposition": "append",
            "columns": {"id": {"name": "id", "data_type": "bigint"}},
        }
    )
    client = HotdataJobClient(schema, _config(), hotdata().capabilities())

    client.initialize_storage(truncate_tables=[])

    assert calls.get("loads") in (None, []), calls.get("loads")


def test_a_partitioned_upsert_table_is_seeded_then_upserts(monkeypatch) -> None:
    """The powercast shape: partitioned AND keyed, loading by upsert.

    The seed matters more here than for append, not less. The server's rule is
    that the FIRST load into a layout-declaring table must be `replace` — it says
    nothing about append specifically — so an upsert-first table hits it too. Once
    the seed has established the layout, the upsert path proceeds normally.
    """
    calls: dict = {}
    api_cls = _recording_api_cls(calls)

    class NewTable(api_cls):  # type: ignore[misc,valid-type]
        def ensure_managed_database(self, **kwargs):
            db = super().ensure_managed_database(**kwargs)
            self.newly_declared = {"orders"}
            return db

    monkeypatch.setattr(jc, "HotdataClient", NewTable)
    schema = Schema("events")
    schema.update_table(
        {
            "name": "orders",
            "write_disposition": "merge",
            "columns": {
                "id": {"name": "id", "data_type": "bigint", "primary_key": True},
                "event_date": {"name": "event_date", "data_type": "date", "partition": True},
                "event_time": {"name": "event_time", "data_type": "timestamp", "sort": True},
            },
        }
    )
    client = HotdataJobClient(schema, _config(), hotdata().capabilities())

    client.initialize_storage(truncate_tables=[])

    # A keyed, partitioned table still gets its zero-row replace seed — the
    # constraint is about the layout, not about the write disposition.
    assert calls.get("loads") == [("orders", "replace", 0)]
    # And the layout itself reached the API alongside the key.
    assert [k.column for k in (calls.get("partition_by") or {}).get("orders", [])] == [
        "event_date"
    ]
    assert (calls.get("keys") or {}).get("orders") == ["id"]


# --- verify_schema: the pre-declaration guard --------------------------------


def _schema_with_layout(**table_hints) -> Schema:
    schema = Schema("events")
    schema.update_table(
        {
            "name": "readings",
            "write_disposition": "append",
            "columns": {
                "event_time": {"name": "event_time", "data_type": "bigint"},
            },
            **table_hints,
        }
    )
    return schema


def test_verify_schema_rejects_a_layout_on_an_absent_column(monkeypatch) -> None:
    """A partition key naming a column the table does not have can never succeed on
    any retry, so it is terminal rather than a warning."""
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls({}))
    client = HotdataJobClient(
        _schema_with_layout(**{"x-hotdata-partition": [{"column": "nope"}]}),
        _config(),
        hotdata().capabilities(),
    )
    with pytest.raises(DestinationTerminalException, match="nope"):
        client.verify_schema(only_tables=["readings"], new_jobs=[])


def test_verify_schema_passes_a_layout_on_a_present_column(monkeypatch) -> None:
    """Also pins the dlt contract this override depends on: that
    JobClientBase.verify_schema takes only_tables / new_jobs as keywords and returns
    an iterable of table schemas. If either drifts, this fails here rather than at
    load time."""
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls({}))
    client = HotdataJobClient(
        _schema_with_layout(**{"x-hotdata-sort": [{"column": "event_time"}]}),
        _config(),
        hotdata().capabilities(),
    )
    loaded = client.verify_schema(only_tables=["readings"], new_jobs=[])
    assert [t["name"] for t in loaded] == ["readings"]


def test_dlt_calls_verify_schema_before_initialize_storage() -> None:
    """The guard is only worth having if it runs before tables are created. dlt's
    init_client calls verify_schema and then _init_dataset_and_update_schema (which
    calls initialize_storage), so a bad layout is caught before any declaration
    reaches the API. Pinned because the ordering is dlt's, not ours."""
    import inspect

    from dlt.load import utils as load_utils

    source = inspect.getsource(load_utils.init_client)
    assert source.index("verify_schema") < source.index(
        "_init_dataset_and_update_schema"
    ), "dlt now initializes storage before verify_schema; the guard must move"


def test_an_invalid_stored_hint_is_terminal_not_a_bare_valueerror(monkeypatch) -> None:
    """A hint the parsers reject raises LayoutError, which subclasses ValueError and
    so cannot be classified as terminal by dlt. Reachable through a hand-edited
    exported schema, which this destination supports on purpose — so it must fail
    the same clean way a bad column name does."""
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls({}))
    client = HotdataJobClient(
        _schema_with_layout(
            **{"x-hotdata-partition": [{"column": "event_time", "transform": "quarter"}]}
        ),
        _config(),
        hotdata().capabilities(),
    )
    with pytest.raises(DestinationTerminalException, match="quarter"):
        client.verify_schema(only_tables=["readings"], new_jobs=[])


def test_an_invalid_stored_hint_is_terminal_in_initialize_storage(monkeypatch) -> None:
    """The other half of the same guard: _table_layouts sits in initialize_storage,
    outside the block that wraps API errors."""
    monkeypatch.setattr(jc, "HotdataClient", _make_fake_api_cls({}))
    client = HotdataJobClient(
        _schema_with_layout(
            **{"x-hotdata-sort": [{"column": "event_time", "direction": "sideways"}]}
        ),
        _config(),
        hotdata().capabilities(),
    )
    with pytest.raises(DestinationTerminalException, match="sideways"):
        client.initialize_storage()


# --- bookkeeping reads and writes ---------------------------------------------


def test_write_schema_skips_a_hash_already_stored(monkeypatch) -> None:
    """One version row per distinct schema. Each row carries a full copy of the
    schema JSON, so a row per load is what made this table grow without bound."""
    store: dict[str, pa.Table] = {}
    client = _client(store, monkeypatch)

    client._write_schema_to_storage()
    assert len(store[VERSION_TABLE_NAME].to_pylist()) == 1

    client._write_schema_to_storage()
    client._write_schema_to_storage()
    assert len(store[VERSION_TABLE_NAME].to_pylist()) == 1

    # A changed schema is a changed hash, which must be recorded.
    client.schema.update_table(
        {"name": "orders", "columns": {"id": {"name": "id", "data_type": "bigint"}}}
    )
    client.schema._bump_version()
    client._write_schema_to_storage()
    rows = store[VERSION_TABLE_NAME].to_pylist()
    assert len(rows) == 2
    assert len({r["version_hash"] for r in rows}) == 2


def test_internal_read_raises_rather_than_reading_as_empty(monkeypatch) -> None:
    """A broken query must not look like "nothing stored".

    If it did, the hash guard above would find no match and write a version row
    on every load — the unbounded growth would come back with nothing to see in
    the logs.
    """
    store: dict[str, pa.Table] = {}

    class BrokenApi(_make_fake_api_cls(store)):  # type: ignore[misc,valid-type]
        def execute_sql(self, sql: str):
            raise HotdataTerminalError("500: internal error")

    monkeypatch.setattr(jc, "HotdataClient", BrokenApi)
    client = HotdataJobClient(Schema("events"), _config(), hotdata().capabilities())

    with pytest.raises(HotdataTerminalError):
        client.get_stored_schema()


def test_internal_read_is_empty_when_the_table_is_not_there_yet(monkeypatch) -> None:
    store: dict[str, pa.Table] = {}

    class MissingTableApi(_make_fake_api_cls(store)):  # type: ignore[misc,valid-type]
        def execute_sql(self, sql: str):
            raise HotdataTerminalError('table "_dlt_version" not found')

    monkeypatch.setattr(jc, "HotdataClient", MissingTableApi)
    client = HotdataJobClient(Schema("events"), _config(), hotdata().capabilities())

    assert client.get_stored_schema() is None


def test_complete_load_appends_a_single_row(monkeypatch) -> None:
    """The row is appended, not merged into a re-upload of the whole table."""
    store: dict[str, pa.Table] = {}
    uploaded: list[tuple[str, str, int]] = []
    base = _make_fake_api_cls(store)

    class Recording(base):  # type: ignore[misc,valid-type]
        def load_managed_table(self, table, *, schema, upload_id, mode="replace", key=None):
            uploaded.append((table, mode, self._pending.num_rows))
            return super().load_managed_table(
                table, schema=schema, upload_id=upload_id, mode=mode, key=key
            )

    monkeypatch.setattr(jc, "HotdataClient", Recording)
    client = HotdataJobClient(Schema("events"), _config(), hotdata().capabilities())

    client.complete_load("load_1")
    client.complete_load("load_2")

    loads_writes = [u for u in uploaded if u[0] == LOADS_TABLE_NAME]
    assert loads_writes == [
        (LOADS_TABLE_NAME, "append", 1),
        (LOADS_TABLE_NAME, "append", 1),
    ]
    assert [r["load_id"] for r in store[LOADS_TABLE_NAME].to_pylist()] == [
        "load_1",
        "load_2",
    ]


def test_state_pruning_keeps_the_newest_rows(monkeypatch) -> None:
    def _state_row(n: int) -> dict:
        return {
            "version": n,
            "engine_version": 1,
            "pipeline_name": "events",
            "state": f"s{n}",
            "created_at": datetime(2024, 1, n, tzinfo=UTC),
            "version_hash": f"h{n}",
            "_dlt_load_id": f"load_{n}",
            "_dlt_id": f"id_{n}",
        }

    store: dict[str, pa.Table] = {
        PIPELINE_STATE_TABLE_NAME: pa.Table.from_pylist([_state_row(n) for n in range(1, 6)])
    }
    client = _client(store, monkeypatch, max_state_files=2)

    client.complete_load("load_5")

    kept = store[PIPELINE_STATE_TABLE_NAME].to_pylist()
    assert sorted(r["_dlt_id"] for r in kept) == ["id_4", "id_5"]


def test_state_pruning_is_off_when_max_state_files_is_zero(monkeypatch) -> None:
    def _state_row(n: int) -> dict:
        return {
            "version": n,
            "engine_version": 1,
            "pipeline_name": "events",
            "state": f"s{n}",
            "created_at": datetime(2024, 1, n, tzinfo=UTC),
            "version_hash": f"h{n}",
            "_dlt_load_id": f"load_{n}",
            "_dlt_id": f"id_{n}",
        }

    store: dict[str, pa.Table] = {
        PIPELINE_STATE_TABLE_NAME: pa.Table.from_pylist([_state_row(n) for n in range(1, 6)])
    }
    client = _client(store, monkeypatch, max_state_files=0)

    client.complete_load("load_5")

    assert len(store[PIPELINE_STATE_TABLE_NAME].to_pylist()) == 5


def test_a_failed_prune_does_not_fail_the_load(monkeypatch) -> None:
    """Retention runs after the load is already complete; it must not undo it."""
    store: dict[str, pa.Table] = {}
    base = _make_fake_api_cls(store)

    class TrimRefused(base):  # type: ignore[misc,valid-type]
        def load_managed_table(self, table, *, schema, upload_id, mode="replace", key=None):
            if table == PIPELINE_STATE_TABLE_NAME:
                raise HotdataTerminalError("500: refused")
            return super().load_managed_table(
                table, schema=schema, upload_id=upload_id, mode=mode, key=key
            )

    store[PIPELINE_STATE_TABLE_NAME] = pa.Table.from_pylist(
        [
            {
                "version": n,
                "engine_version": 1,
                "pipeline_name": "events",
                "state": f"s{n}",
                "created_at": datetime(2024, 1, n, tzinfo=UTC),
                "version_hash": f"h{n}",
                "_dlt_load_id": f"load_{n}",
                "_dlt_id": f"id_{n}",
            }
            for n in range(1, 6)
        ]
    )
    monkeypatch.setattr(jc, "HotdataClient", TrimRefused)
    client = HotdataJobClient(
        Schema("events"), _config(max_state_files=2), hotdata().capabilities()
    )

    client.complete_load("load_5")

    # The load row landed even though the trim could not run, and the state rows
    # are left exactly as they were rather than half-rewritten.
    assert [r["load_id"] for r in store[LOADS_TABLE_NAME].to_pylist()] == ["load_5"]
    assert len(store[PIPELINE_STATE_TABLE_NAME].to_pylist()) == 5
