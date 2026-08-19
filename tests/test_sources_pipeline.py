"""A Hotdata source resource driven through a real dlt pipeline.

Proves the resources are loadable, not just constructible: dlt extracts,
normalises and loads them. Only the read transport is faked -- everything above
it is real dlt, and the rows are read back off disk rather than asserted from the
pipeline's own report.

The filesystem destination is used rather than a database one so the test adds no
dependency to the package it is testing. Parquet is the loader format because the
resources yield Arrow: it is the round-trip a real user gets, and it keeps the
types the engine reported instead of routing them through JSON.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import hotdata_dlt_destination.sources as sources
from hotdata_dlt_destination.source_client import IncompleteReadError, _sliced
from hotdata_dlt_destination.sources import hotdata_query, hotdata_table

if TYPE_CHECKING:
    from collections.abc import Iterator

SOURCE = pa.table(
    {
        "order_id": ["ord_1", "ord_2", "ord_3"],
        "amount": [10.5, 20.25, 30.0],
        "region": ["emea", "apac", "emea"],
    }
)

CREDS = {
    "database_id": "dbid_source",
    "workspace_id": "ws_1",
    "credentials": {"api_key": "secret"},
}


@pytest.fixture
def faked_read(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace the module's batch reader; record the SQL it was asked to run."""
    seen: list[str] = []

    def fake_batches(*, sql: str, batch_rows: int, **_: object) -> Iterator[pa.RecordBatch]:
        seen.append(sql)
        for batch in SOURCE.to_batches():
            yield from _sliced(batch, batch_rows)

    monkeypatch.setattr(sources, "_batches", fake_batches)
    return seen


def _pipeline(name: str, tmp_path: Path) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=name,
        destination=dlt.destinations.filesystem(str(tmp_path / "out")),
        dataset_name="copied",
        dev_mode=True,
    )


def _loaded_rows(tmp_path: Path, table: str) -> list[dict[str, object]]:
    """Every row the load wrote for `table`, read back off disk.

    Read from the files rather than from the pipeline's report: the report says a
    job succeeded, the files say what a reader of the destination would actually
    see.
    """
    files = sorted((tmp_path / "out").rglob(f"**/{table}/*.parquet"))
    assert files, f"no parquet written for {table}"
    rows: list[dict[str, object]] = []
    for path in files:
        rows.extend(pq.read_table(path).to_pylist())
    return rows


def test_a_table_resource_loads_into_a_pipeline(faked_read: list[str], tmp_path: Path) -> None:
    info = _pipeline("hotdata_source_table", tmp_path).run(
        hotdata_table("orders", **CREDS), loader_file_format="parquet"
    )
    assert not info.has_failed_jobs
    assert faked_read == ["SELECT * FROM public.orders"]

    rows = _loaded_rows(tmp_path, "orders")
    assert sorted(r["order_id"] for r in rows) == ["ord_1", "ord_2", "ord_3"]
    # Arrow round-trip, not a stringified one: the float stays a float.
    amounts = {r["order_id"]: r["amount"] for r in rows}
    assert amounts["ord_1"] == pytest.approx(10.5)


def test_a_query_resource_loads_under_its_own_name(
    faked_read: list[str], tmp_path: Path
) -> None:
    pipeline = _pipeline("hotdata_source_query", tmp_path)
    info = pipeline.run(
        hotdata_query("select order_id, amount from public.orders", name="order_amounts", **CREDS),
        loader_file_format="parquet",
    )
    assert not info.has_failed_jobs
    assert faked_read == ["select order_id, amount from public.orders"]
    assert "order_amounts" in pipeline.default_schema.tables
    assert len(_loaded_rows(tmp_path, "order_amounts")) == 3


def test_selected_columns_reach_the_read(faked_read: list[str], tmp_path: Path) -> None:
    _pipeline("hotdata_source_cols", tmp_path).run(
        hotdata_table("orders", included_columns=["order_id", "amount"], **CREDS),
        loader_file_format="parquet",
    )
    assert faked_read == ["SELECT order_id, amount FROM public.orders"]


def test_merge_hints_reach_the_loaded_schema(faked_read: list[str], tmp_path: Path) -> None:
    """Resource hints are the only thing that carries the key to the destination.

    Without them a merge falls back to dlt's own row id, which is fresh per
    extract -- so a re-read would duplicate rather than overwrite.
    """
    pipeline = _pipeline("hotdata_source_merge", tmp_path)
    pipeline.run(
        hotdata_table("orders", primary_key="order_id", write_disposition="merge", **CREDS),
        loader_file_format="parquet",
    )
    table = pipeline.default_schema.tables["orders"]
    assert table["write_disposition"] == "merge"
    assert table["columns"]["order_id"]["primary_key"] is True


def test_an_incomplete_read_fails_the_load_rather_than_loading_a_prefix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The whole point, seen from the pipeline.

    A read that returns part of a result must not produce a successful load. If
    it did, the pipeline would advance its state over rows it never saw and
    nothing downstream would look for them again.
    """

    def short_read(*, sql: str, batch_rows: int, **_: object) -> Iterator[pa.RecordBatch]:
        yield SOURCE.slice(0, 1).to_batches()[0]
        raise IncompleteReadError(result_id="rslt_1", expected=3, received=1)

    monkeypatch.setattr(sources, "_batches", short_read)
    with pytest.raises(Exception, match="rslt_1"):
        _pipeline("hotdata_source_short", tmp_path).run(
            hotdata_table("orders", **CREDS), loader_file_format="parquet"
        )
