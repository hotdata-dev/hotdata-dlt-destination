"""End-to-end tests: real dlt pipelines through the real `hotdata` destination,
`HotdataJobClient`, and `HotdataClient`, backed by an in-memory managed-database
simulator (only the network transport is faked, so no credentials are needed).

These exercise the integration paths unit tests can't: state-sync round-trip,
nested/child tables, dlt bookkeeping, write dispositions, and the data-preserving
schema-evolution recreate.
"""

from __future__ import annotations

import datetime
import re
from types import SimpleNamespace

import dlt
import hotdata_framework.managed_client as mc
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.schema import Schema
from hotdata.models.query_response import QueryResponse

import hotdata_dlt_destination.job_client as jc
import hotdata_dlt_destination.sql_client as sc
from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import HotdataClientConfiguration, HotdataCredentials
from hotdata_dlt_destination.hotdata_client import HotdataClient as RealHotdataClient

_ACTIVE: dict[str, InMemoryBackend] = {}


class InMemoryBackend:
    """Stands in for the hotdata-framework runtime client."""

    api = None

    def __init__(self) -> None:
        self.name_to_id: dict[str, str] = {}
        self.id_to_name: dict[str, str] = {}
        self.declared: dict[str, set[str]] = {}
        self.keys: dict[tuple, list[str]] = {}
        self.tables: dict[tuple, pa.Table] = {}
        self.uploads: dict[str, pa.Table] = {}
        self.results: dict[str, pa.Table] = {}
        self.load_counts: dict[tuple, int] = {}
        self._n = 0

    def close(self) -> None:
        pass

    def resolve_managed_database(self, name_or_id):
        name = self.id_to_name.get(name_or_id, name_or_id)
        if name not in self.name_to_id:
            raise KeyError(name)
        return SimpleNamespace(id=self.name_to_id[name], default_connection_id="conn")

    def list_managed_tables(self, database, *, schema=None):
        name = self.id_to_name.get(database, database)
        return [
            SimpleNamespace(table=t, var_schema="public", synced=(name, "public", t) in self.tables)
            for t in sorted(self.declared.get(name, set()))
        ]

    def create_managed_database(self, *, description, schema, tables, keys=None, expires_at=None):
        self._n += 1
        db_id = f"db_{self._n}"
        self.name_to_id[description] = db_id
        self.id_to_name[db_id] = description
        self.declared[description] = set(tables)
        for t in tables:
            self.keys[(description, schema, t)] = list((keys or {}).get(t, []))
        return SimpleNamespace(id=db_id, default_connection_id="conn")

    def delete_managed_database(self, name_or_id):
        name = self.id_to_name.get(name_or_id, name_or_id)
        db_id = self.name_to_id.pop(name, None)
        self.id_to_name.pop(db_id, None)
        self.declared.pop(name, None)
        for key in [k for k in self.tables if k[0] == name]:
            del self.tables[key]

    def add_managed_table(self, database, table, *, schema, key=None):
        name = self.id_to_name.get(database, database)
        self.declared.setdefault(name, set()).add(table)
        self.keys[(name, schema, table)] = list(key or [])
        return SimpleNamespace(table=table, var_schema=schema, synced=False)

    def delete_managed_table(self, database, table, *, schema="public"):
        name = self.id_to_name.get(database, database)
        self.declared.get(name, set()).discard(table)
        self.tables.pop((name, schema, table), None)
        self.keys.pop((name, schema, table), None)

    def upload_parquet(self, path):
        self._n += 1
        uid = f"up_{self._n}"
        self.uploads[uid] = pq.read_table(path)
        return uid

    def load_managed_table(self, database, table, *, schema, upload_id, mode="replace"):
        import pyarrow as _pa

        name = self.id_to_name.get(database, database)
        k = (name, schema, table)
        self.load_counts[k] = self.load_counts.get(k, 0) + 1
        incoming = self.uploads[upload_id]
        existing = self.tables.get(k)

        if mode == "replace" or existing is None:
            result = incoming
        elif mode == "append":
            result = _pa.concat_tables([existing, incoming], promote_options="permissive")
        else:
            # key-based modes: match rows by the table's declared key.
            key_cols = self.keys.get(k, [])
            ex_rows, inc_rows = existing.to_pylist(), incoming.to_pylist()

            def _kt(row: dict) -> tuple:
                return tuple(row.get(c) for c in key_cols)

            if mode == "delete":
                drop = {_kt(r) for r in inc_rows}
                kept = [r for r in ex_rows if _kt(r) not in drop]
                result = (
                    _pa.Table.from_pylist(kept, schema=existing.schema)
                    if kept
                    else existing.schema.empty_table()
                )
            else:  # update (ignore unmatched) / upsert (also insert unmatched)
                index = {_kt(r): i for i, r in enumerate(ex_rows)}
                merged = list(ex_rows)
                for r in inc_rows:
                    kt = _kt(r)
                    if kt in index:
                        merged[index[kt]] = r
                    elif mode == "upsert":
                        index[kt] = len(merged)
                        merged.append(r)
                unified = _pa.unify_schemas(
                    [existing.schema, incoming.schema], promote_options="permissive"
                )
                result = _pa.Table.from_pylist(merged, schema=unified)

        self.tables[k] = result
        return SimpleNamespace(full_name=f"{name}.{schema}.{table}")

    def execute_sql(self, database, sql):
        """Execute SQL over this database's stored Arrow tables.

        A plain ``SELECT * FROM "default"."<schema>"."<table>"`` (what the write
        path's ``fetch_table`` emits) short-circuits to the exact stored table so
        the write-path tests see byte-identical data. Anything richer (the dataset
        read path: WHERE / LIMIT / aggregates / column projection) runs on a real
        DataFusion session so dialect behavior matches the production engine.
        """
        m = re.fullmatch(
            r'\s*SELECT \* FROM "default"\."([^"]+)"\."([^"]+)"\s*', sql, flags=re.IGNORECASE
        )
        if m:
            return self.tables[(database, m.group(1), m.group(2))]

        from datafusion import SessionContext

        ctx = SessionContext()
        for (db, _schema, table), tbl in self.tables.items():
            if db == database:
                ctx.register_record_batches(table, [tbl.to_batches()])
        # Strip the "default"."<schema>". qualifier so bare table names resolve.
        rewritten = re.sub(r'"default"\."[^"]+"\.(?=")', "", sql)
        return ctx.sql(rewritten).to_arrow_table()

    def store_result(self, table):
        self._n += 1
        rid = f"res_{self._n}"
        self.results[rid] = table
        return rid

    # test helper
    def rows(self, database, table, schema="public"):
        t = self.tables.get((database, schema, table))
        return t.to_pylist() if t is not None else None


class _FakeQueryApi:
    def __init__(self, api):
        pass

    def query(self, request, *, x_database_id):
        be = _ACTIVE["backend"]
        database = be.id_to_name[x_database_id]
        result = be.execute_sql(database, request.sql)
        return QueryResponse(
            columns=[],
            rows=[],
            row_count=result.num_rows,
            preview_row_count=0,
            truncated=False,
            nullable=[],
            result_id=be.store_result(result),
            query_run_id="qr",
            execution_time_ms=1,
        )


class _FakeResultsApi:
    def __init__(self, api):
        pass

    def get_result(self, result_id, *, x_database_id=None):
        return SimpleNamespace(status="ready", result_id=result_id, error_message=None)


class _FakeArrowResultsApi:
    def __init__(self, api):
        pass

    # x_database_id is required in the hotdata 0.6.0 SDK (results of
    # database-scoped queries are database-scoped); framework >=0.6.1 and
    # execute_sql pass it on every result read.
    def get_result_arrow(self, result_id, *, x_database_id):
        be = _ACTIVE["backend"]
        assert x_database_id in be.id_to_name, x_database_id
        return be.results[result_id]


class _E2EClient(RealHotdataClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._runtime = _ACTIVE["backend"]


@pytest.fixture
def backend(monkeypatch):
    be = InMemoryBackend()
    _ACTIVE["backend"] = be
    monkeypatch.setattr(mc, "QueryApi", _FakeQueryApi)
    monkeypatch.setattr(mc, "ResultsApi", _FakeResultsApi)
    monkeypatch.setattr(mc, "ArrowResultsApi", _FakeArrowResultsApi)
    # hotdata_client.py also references ArrowResultsApi (for execute_sql).
    monkeypatch.setattr(
        "hotdata_dlt_destination.hotdata_client.ArrowResultsApi", _FakeArrowResultsApi
    )
    monkeypatch.setattr(jc, "HotdataClient", _E2EClient)
    monkeypatch.setattr(sc, "HotdataClient", _E2EClient)
    yield be
    _ACTIVE.pop("backend", None)


def _dest(database_name, declared_tables, write_disposition="append"):
    return hotdata(
        credentials=HotdataCredentials(api_key="test", workspace_id="ws_test"),
        database_name=database_name,
        declared_tables=declared_tables,
        write_disposition=write_disposition,
    )


def test_load_replace_and_bookkeeping(backend, tmp_path):
    @dlt.resource(name="orders", write_disposition="replace")
    def orders():
        yield [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}]

    dlt.pipeline(
        pipeline_name="p_basic",
        destination=_dest("e2e_basic", ["orders"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    ).run(orders())

    rows = backend.rows("e2e_basic", "orders")
    assert rows is not None and len(rows) == 2
    assert "_dlt_id" in rows[0] and "_dlt_load_id" in rows[0]
    assert backend.rows("e2e_basic", "_dlt_loads") is not None
    assert backend.rows("e2e_basic", "_dlt_version") is not None


def test_replace_multi_file_pipeline_keeps_all_rows(backend, tmp_path, monkeypatch):
    # Regression for multi-file replace data loss: with a small
    # DATA_WRITER__FILE_MAX_BYTES (the ingest worker sets 200MB in production),
    # dlt splits a replace table across several parquet files, one load job per
    # file. Per-file mode="replace" left only the LAST file's rows; the fix
    # truncates once at package init and appends every file.
    monkeypatch.setenv("DATA_WRITER__FILE_MAX_BYTES", "256")
    monkeypatch.setenv("DATA_WRITER__BUFFER_MAX_ITEMS", "10")

    @dlt.resource(name="rows", write_disposition="replace")
    def rows_v1():
        yield from ({"id": i, "payload": "x" * 40} for i in range(100))

    pipe = dlt.pipeline(
        pipeline_name="p_multifile",
        destination=_dest("e2e_multifile", ["rows"], "replace"),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    )
    pipe.run(rows_v1())

    k = ("e2e_multifile", "public", "rows")
    assert backend.load_counts.get(k, 0) >= 2, "table must span multiple files to exercise the bug"
    loaded = backend.rows("e2e_multifile", "rows")
    assert sorted(r["id"] for r in loaded) == list(range(100))

    # A second run must still fully replace, not accumulate.
    @dlt.resource(name="rows", write_disposition="replace")
    def rows_v2():
        yield from ({"id": i, "payload": "y" * 40} for i in range(100, 150))

    pipe.run(rows_v2())
    loaded = backend.rows("e2e_multifile", "rows")
    assert sorted(r["id"] for r in loaded) == list(range(100, 150))


def test_load_decimal_column(backend, tmp_path):
    # Regression: capabilities left decimal_precision unset, so normalizing a
    # Decimal column without explicit precision hints crashed to parquet
    # (get_py_arrow_numeric: "'NoneType' object is not subscriptable"). The value
    # must load and round-trip using the destination's default numeric precision.
    from decimal import Decimal

    @dlt.resource(name="inventory", write_disposition="replace")
    def inventory():
        yield [{"id": 1, "price": Decimal("1.50")}, {"id": 2, "price": Decimal("2.50")}]

    dlt.pipeline(
        pipeline_name="p_decimal",
        destination=_dest("e2e_decimal", ["inventory"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    ).run(inventory())

    rows = backend.rows("e2e_decimal", "inventory")
    assert rows is not None
    by_id = {r["id"]: r["price"] for r in rows}
    assert by_id[1] == Decimal("1.50")
    assert by_id[2] == Decimal("2.50")


def test_nested_child_tables(backend, tmp_path):
    @dlt.resource(name="orders", write_disposition="replace")
    def orders():
        yield [{"id": 1, "items": [{"sku": "a"}, {"sku": "b"}]}, {"id": 2, "items": [{"sku": "c"}]}]

    dlt.pipeline(
        pipeline_name="p_nested",
        destination=_dest("e2e_nested", ["orders"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    ).run(orders())

    child = backend.rows("e2e_nested", "orders__items")
    assert child is not None, "nested child table orders__items must be created (no double prefix)"
    assert len(child) == 3


def test_state_sync_roundtrip(backend, tmp_path):
    @dlt.resource(name="events", write_disposition="append", primary_key="id")
    def events(updated=dlt.sources.incremental("updated")):  # noqa: B008  (dlt idiom)
        yield [{"id": 1, "updated": 5}, {"id": 2, "updated": 7}]

    dlt.pipeline(
        pipeline_name="p_state",
        destination=_dest("e2e_state", ["events"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    ).run(events())

    assert backend.rows("e2e_state", "_dlt_pipeline_state") is not None

    cfg = HotdataClientConfiguration(
        credentials=HotdataCredentials(api_key="test", workspace_id="ws_test"),
        database_name="e2e_state",
        schema="public",
    )
    client = jc.HotdataJobClient(Schema("p_state"), cfg, hotdata().capabilities())
    stored = client.get_stored_state("p_state")
    assert stored is not None and stored.pipeline_name == "p_state"


def test_merge_dedup(backend, tmp_path):
    @dlt.resource(name="users", write_disposition="merge", primary_key="id")
    def users(batch):
        yield batch

    pipe = dlt.pipeline(
        pipeline_name="p_merge",
        destination=_dest("e2e_merge", ["users"], "merge"),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    )
    pipe.run(users([{"id": 1, "name": "old"}, {"id": 2, "name": "keep"}]))
    pipe.run(users([{"id": 1, "name": "new"}, {"id": 3, "name": "added"}]))

    by_id = {r["id"]: r["name"] for r in backend.rows("e2e_merge", "users")}
    assert by_id == {1: "new", 2: "keep", 3: "added"}


def _spans_pipeline(tmp_path):
    @dlt.resource(name="spans", write_disposition="replace")
    def spans():
        yield [
            {"span_id": "a1", "model": "claude-opus-4-8", "latency_ms": 812, "ok": True},
            {"span_id": "a2", "model": "claude-sonnet-5", "latency_ms": 240, "ok": True},
            {"span_id": "a3", "model": "claude-opus-4-8", "latency_ms": 590, "ok": False},
        ]

    pipe = dlt.pipeline(
        pipeline_name="p_read",
        destination=_dest("e2e_read", ["spans"], "replace"),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    )
    pipe.run(spans())
    return pipe


def test_dataset_read_roundtrip_df(backend, tmp_path):
    pipe = _spans_pipeline(tmp_path)
    df = pipe.dataset().table("spans").df()
    got = {(r.span_id, r.model, r.latency_ms, r.ok) for r in df.itertuples()}
    assert got == {
        ("a1", "claude-opus-4-8", 812, True),
        ("a2", "claude-sonnet-5", 240, True),
        ("a3", "claude-opus-4-8", 590, False),
    }


def test_dataset_read_arrow(backend, tmp_path):
    pipe = _spans_pipeline(tmp_path)
    tbl = pipe.dataset().table("spans").arrow()
    assert tbl.num_rows == 3
    assert {"span_id", "model", "latency_ms", "ok"} <= set(tbl.column_names)


def test_dataset_raw_sql_aggregate(backend, tmp_path):
    pipe = _spans_pipeline(tmp_path)
    df = pipe.dataset()(
        "SELECT model, avg(latency_ms) AS p FROM spans GROUP BY model ORDER BY model"
    ).df()
    by_model = {r.model: r.p for r in df.itertuples()}
    assert by_model["claude-sonnet-5"] == 240
    assert by_model["claude-opus-4-8"] == (812 + 590) / 2


def test_dataset_fluent_filters(backend, tmp_path):
    pipe = _spans_pipeline(tmp_path)
    df = pipe.dataset().table("spans").select("span_id", "latency_ms").where("ok").order_by(
        "latency_ms"
    ).limit(1).df()
    assert list(df["span_id"]) == ["a2"]
    assert list(df["latency_ms"]) == [240]


def test_dataset_row_counts(backend, tmp_path):
    pipe = _spans_pipeline(tmp_path)
    counts = pipe.dataset().row_counts(table_names=["spans"]).df()
    row = {r.table_name: r.row_count for r in counts.itertuples()}
    assert row["spans"] == 3


def test_ibis_expression_reads(backend, tmp_path):
    # ibis expressions compile to SQL (postgres dialect) and run through the
    # sql_client — no live ibis backend needed, no destination-side code.
    pytest.importorskip("ibis")
    pipe = _spans_pipeline(tmp_path)
    ds = pipe.dataset()
    t = ds.table("spans").to_ibis()
    expr = t.group_by("model").aggregate(avg_latency_ms=t.latency_ms.mean()).order_by("model")
    by = {r.model: r.avg_latency_ms for r in ds(expr).df().itertuples()}
    assert by["claude-sonnet-5"] == 240
    assert by["claude-opus-4-8"] == (812 + 590) / 2


def test_ibis_live_backend(backend, tmp_path):
    # dataset().ibis() returns a live ibis.hotdata backend, with config mapped
    # onto the connection and the managed database bound by id. No query runs, so
    # nothing hits the network.
    pytest.importorskip("ibis")
    pytest.importorskip("ibis_hotdata")
    pipe = _spans_pipeline(tmp_path)
    con = pipe.dataset().ibis()
    assert con.name == "hotdata"
    assert con._default_schema == "public"
    assert con._database_id == backend.name_to_id["e2e_read"]


def test_ibis_backend_delegates_non_hotdata(monkeypatch):
    # A non-Hotdata client falls through to dlt's original create_ibis_backend.
    import hotdata_dlt_destination.ibis_backend as ib

    captured = {}

    def fake_original(destination, client, read_only=False, schemas=()):
        captured["read_only"] = read_only
        return "delegated"

    monkeypatch.setattr(ib, "_original_create_ibis_backend", fake_original)
    result = ib._create_ibis_backend("duckdb", object(), read_only=True, schemas=())
    assert result == "delegated"
    assert captured["read_only"] is True


def test_install_ibis_backend_idempotent():
    # Re-installing leaves our wrapper in place and never captures itself as the
    # delegated original.
    pytest.importorskip("ibis")
    import dlt.helpers.ibis as dlt_ibis

    import hotdata_dlt_destination.ibis_backend as ib

    ib.install_ibis_backend()
    ib.install_ibis_backend()
    assert dlt_ibis.create_ibis_backend is ib._create_ibis_backend
    assert ib._original_create_ibis_backend is not ib._create_ibis_backend


def test_has_dataset_true_after_load(backend, tmp_path):
    pipe = _spans_pipeline(tmp_path)
    with pipe.dataset().sql_client as client:
        assert client.has_dataset() is True


def test_read_preserves_column_types(backend, tmp_path):
    # We never inspect or coerce values -- the cursor hands the engine's Arrow
    # table straight to pandas/arrow. This guards the *pipeline* (dlt normalize ->
    # parquet -> DataFusion -> Arrow -> pandas) preserving representative types.
    import pyarrow as pa

    @dlt.resource(name="typed", write_disposition="replace")
    def typed():
        yield [
            {
                "i": 1,
                "f": 1.5,
                "b": True,
                "s": "alpha",
                "ts": datetime.datetime(2026, 7, 7, 12, 0, tzinfo=datetime.UTC),
                "d": datetime.date(2026, 7, 7),
                "maybe": 10,
            },
            {
                "i": 2,
                "f": 2.5,
                "b": False,
                "s": "beta",
                "ts": datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                "d": datetime.date(2026, 1, 1),
                "maybe": None,
            },
        ]

    pipe = dlt.pipeline(
        pipeline_name="p_types",
        destination=_dest("e2e_types", ["typed"], "replace"),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    )
    pipe.run(typed())

    tbl = pipe.dataset().table("typed").arrow()
    by = {f.name: f.type for f in tbl.schema}

    assert pa.types.is_integer(by["i"])
    assert pa.types.is_floating(by["f"])
    assert pa.types.is_boolean(by["b"])
    assert pa.types.is_string(by["s"]) or pa.types.is_large_string(by["s"])
    assert pa.types.is_timestamp(by["ts"])
    assert pa.types.is_date(by["d"]) or pa.types.is_timestamp(by["d"])
    assert pa.types.is_integer(by["maybe"])  # nullable int column stays integer

    df = pipe.dataset().table("typed").order_by("i").df()
    assert list(df["i"]) == [1, 2]
    assert list(df["b"]) == [True, False]
    assert list(df["s"]) == ["alpha", "beta"]
    assert df["maybe"].isna().sum() == 1  # the null survived


def test_schema_evolution_preserves_data(backend, tmp_path):
    @dlt.resource(name="orders", write_disposition="append")
    def orders():
        yield [{"id": 1, "amount": 100}]

    @dlt.resource(name="customers", write_disposition="append")
    def customers():
        yield [{"id": 9, "name": "acme"}]

    dlt.pipeline(
        pipeline_name="p_evo",
        destination=_dest("e2e_evo", ["orders"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path / "r1"),
    ).run(orders())
    assert len(backend.rows("e2e_evo", "orders")) == 1
    db_id_before = backend.name_to_id["e2e_evo"]

    # Adding a new table declares it in place (no recreate); existing data survives.
    dlt.pipeline(
        pipeline_name="p_evo",
        destination=_dest("e2e_evo", ["orders", "customers"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path / "r2"),
    ).run([customers()])

    # The database was never deleted/recreated — same id throughout.
    assert backend.name_to_id["e2e_evo"] == db_id_before
    orders_after = backend.rows("e2e_evo", "orders")
    assert orders_after is not None and len(orders_after) == 1 and orders_after[0]["id"] == 1
    assert backend.rows("e2e_evo", "customers") is not None
    assert backend.rows("e2e_evo", "_dlt_pipeline_state") is not None


def test_hyphenated_database_name_stays_one_database(backend, tmp_path):
    # database_name is an opaque API address (a name or a dbid), not a SQL
    # identifier. The load jobs used to snake_case it while schema/state/
    # bookkeeping writes used it verbatim, so a hyphenated name minted a twin
    # database that silently received all the data — reads against the real DB
    # then failed with "declared but has no data" (regression, 2026-07-09).
    @dlt.resource(name="rows", write_disposition="replace")
    def rows():
        yield [{"n": 1}, {"n": 2}]

    pipe = dlt.pipeline(
        pipeline_name="p_hyphen",
        destination=_dest("e2e-hyphen-db", ["rows"], "replace"),
        dataset_name="public",
        pipelines_dir=str(tmp_path),
    )
    pipe.run(rows())

    # Exactly one database, under the exact name the caller addressed.
    assert set(backend.name_to_id) == {"e2e-hyphen-db"}
    # And the data is readable back through that same address.
    df = pipe.dataset().table("rows").df()
    assert sorted(df["n"].tolist()) == [1, 2]
