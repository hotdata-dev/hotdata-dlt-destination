import pyarrow as pa
import pytest

from hotdata_dlt_destination.merge import (
    combine_tables,
    merge_rows,
    resolve_primary_key,
    resolve_write_disposition,
)


def test_resolve_write_disposition_prefers_table_schema() -> None:
    table = {"name": "orders", "write_disposition": "merge"}
    assert resolve_write_disposition(table, "append") == "merge"


def test_resolve_primary_key_supports_composite_keys() -> None:
    table = {"name": "orders", "primary_key": ["tenant_id", "id"]}
    assert resolve_primary_key(table) == ["tenant_id", "id"]


def test_merge_rows_updates_matching_primary_key() -> None:
    existing = [{"id": 1, "value": "old"}, {"id": 2, "value": "keep"}]
    incoming = [{"id": 1, "value": "new"}]
    merged = merge_rows(existing, incoming, primary_key=["id"])
    assert merged == [{"id": 1, "value": "new"}, {"id": 2, "value": "keep"}]


# --- combine_tables ---


def _t(*rows: dict) -> pa.Table:
    return pa.Table.from_pylist(list(rows))


def test_combine_tables_replace_returns_incoming() -> None:
    result = combine_tables(
        disposition="replace",
        existing=_t({"id": 1}),
        incoming=_t({"id": 2}),
        primary_key=["id"],
    )
    assert result.to_pylist() == [{"id": 2}]


def test_combine_tables_existing_none_returns_incoming() -> None:
    incoming = _t({"id": 1})
    result = combine_tables(
        disposition="append",
        existing=None,
        incoming=incoming,
        primary_key=None,
    )
    assert result.to_pylist() == [{"id": 1}]


def test_combine_tables_empty_existing_returns_incoming() -> None:
    result = combine_tables(
        disposition="append",
        existing=pa.table({"id": pa.array([], type=pa.int64())}),
        incoming=_t({"id": 1}),
        primary_key=None,
    )
    assert result.to_pylist() == [{"id": 1}]


def test_combine_tables_append_concatenates() -> None:
    result = combine_tables(
        disposition="append",
        existing=_t({"id": 1, "v": "a"}),
        incoming=_t({"id": 2, "v": "b"}),
        primary_key=None,
    )
    assert result.to_pylist() == [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


def test_combine_tables_append_fills_missing_columns_with_null() -> None:
    # Schema drift: existing has col "extra" that incoming lacks.
    existing = _t({"id": 1, "extra": "x"})
    incoming = _t({"id": 2})
    result = combine_tables(
        disposition="append",
        existing=existing,
        incoming=incoming,
        primary_key=None,
    )
    rows = result.to_pylist()
    assert rows[0] == {"id": 1, "extra": "x"}
    assert rows[1]["id"] == 2
    assert rows[1].get("extra") is None


def test_combine_tables_merge_updates_by_primary_key() -> None:
    result = combine_tables(
        disposition="merge",
        existing=_t({"id": 1, "v": "old"}, {"id": 2, "v": "keep"}),
        incoming=_t({"id": 1, "v": "new"}),
        primary_key=["id"],
    )
    assert result.to_pylist() == [{"id": 1, "v": "new"}, {"id": 2, "v": "keep"}]


def test_combine_tables_upsert_same_as_merge() -> None:
    result = combine_tables(
        disposition="upsert",
        existing=_t({"id": 1, "v": "old"}),
        incoming=_t({"id": 1, "v": "new"}, {"id": 2, "v": "added"}),
        primary_key=["id"],
    )
    assert result.to_pylist() == [{"id": 1, "v": "new"}, {"id": 2, "v": "added"}]


def test_combine_tables_merge_falls_back_to_hotdata_row_key() -> None:
    result = combine_tables(
        disposition="merge",
        existing=_t({"_hotdata_row_key": "a", "value": 1}),
        incoming=_t({"_hotdata_row_key": "a", "value": 2}),
        primary_key=None,
    )
    assert result.to_pylist() == [{"_hotdata_row_key": "a", "value": 2}]


def test_combine_tables_rejects_unknown_disposition() -> None:
    with pytest.raises(ValueError, match="Unsupported write_disposition 'appned'"):
        combine_tables(
            disposition="appned",
            existing=_t({"id": 1}),
            incoming=_t({"id": 2}),
            primary_key=["id"],
        )
