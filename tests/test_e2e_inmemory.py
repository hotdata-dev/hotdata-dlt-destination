"""End-to-end tests: real dlt pipelines through the real `hotdata` destination,
`HotdataJobClient`, and `HotdataClient`, backed by an in-memory managed-database
simulator (only the network transport is faked, so no credentials are needed).

These exercise the integration paths unit tests can't: state-sync round-trip,
nested/child tables, dlt bookkeeping, write dispositions, and the data-preserving
schema-evolution recreate.
"""

from __future__ import annotations

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
        self.tables: dict[tuple, pa.Table] = {}
        self.uploads: dict[str, pa.Table] = {}
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

    def create_managed_database(self, *, description, schema, tables, expires_at=None):
        self._n += 1
        db_id = f"db_{self._n}"
        self.name_to_id[description] = db_id
        self.id_to_name[db_id] = description
        self.declared[description] = set(tables)
        return SimpleNamespace(id=db_id, default_connection_id="conn")

    def delete_managed_database(self, name_or_id):
        name = self.id_to_name.get(name_or_id, name_or_id)
        db_id = self.name_to_id.pop(name, None)
        self.id_to_name.pop(db_id, None)
        self.declared.pop(name, None)
        for key in [k for k in self.tables if k[0] == name]:
            del self.tables[key]

    def upload_parquet(self, path):
        self._n += 1
        uid = f"up_{self._n}"
        self.uploads[uid] = pq.read_table(path)
        return uid

    def load_managed_table(self, database, table, *, schema, upload_id):
        name = self.id_to_name.get(database, database)
        self.tables[(name, schema, table)] = self.uploads[upload_id]
        return SimpleNamespace(full_name=f"{name}.{schema}.{table}")

    # test helper
    def rows(self, database, table, schema="public"):
        t = self.tables.get((database, schema, table))
        return t.to_pylist() if t is not None else None


class _FakeQueryApi:
    def __init__(self, api):
        pass

    def query(self, request, *, x_database_id):
        m = re.search(r'"default"\."([^"]+)"\."([^"]+)"', request.sql)
        return QueryResponse(
            columns=[],
            rows=[],
            row_count=0,
            preview_row_count=0,
            truncated=False,
            nullable=[],
            result_id=f"{x_database_id}|{m.group(1)}|{m.group(2)}",
            query_run_id="qr",
            execution_time_ms=1,
        )


class _FakeArrowResultsApi:
    def __init__(self, api):
        pass

    def get_result_arrow(self, result_id):
        db_id, schema, table = result_id.split("|")
        be = _ACTIVE["backend"]
        return be.tables[(be.id_to_name[db_id], schema, table)]


class _E2EClient(RealHotdataClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._runtime = _ACTIVE["backend"]


@pytest.fixture
def backend(monkeypatch):
    be = InMemoryBackend()
    _ACTIVE["backend"] = be
    monkeypatch.setattr(mc, "QueryApi", _FakeQueryApi)
    monkeypatch.setattr(mc, "ArrowResultsApi", _FakeArrowResultsApi)
    monkeypatch.setattr(jc, "HotdataClient", _E2EClient)
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

    # Adding a new table triggers the union-recreate; existing data must survive.
    dlt.pipeline(
        pipeline_name="p_evo",
        destination=_dest("e2e_evo", ["orders", "customers"]),
        dataset_name="public",
        pipelines_dir=str(tmp_path / "r2"),
    ).run([customers()])

    orders_after = backend.rows("e2e_evo", "orders")
    assert orders_after is not None and len(orders_after) == 1 and orders_after[0]["id"] == 1
    assert backend.rows("e2e_evo", "customers") is not None
    assert backend.rows("e2e_evo", "_dlt_pipeline_state") is not None
