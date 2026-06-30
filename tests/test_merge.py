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


def test_combine_tables_insert_only_skips_existing_keys() -> None:
    result = combine_tables(
        disposition="insert-only",
        existing=_t({"id": 1, "v": "a"}),
        incoming=_t({"id": 1, "v": "b"}, {"id": 2, "v": "c"}),
        primary_key=["id"],
    )
    # existing id=1 is preserved (not overwritten); only the new id=2 is appended
    assert result.to_pylist() == [{"id": 1, "v": "a"}, {"id": 2, "v": "c"}]


def test_combine_tables_insert_only_returns_existing_when_no_new_rows() -> None:
    existing = _t({"id": 1, "v": "a"})
    result = combine_tables(
        disposition="insert-only",
        existing=existing,
        incoming=_t({"id": 1, "v": "b"}),
        primary_key=["id"],
    )
    assert result.to_pylist() == [{"id": 1, "v": "a"}]


def test_combine_tables_merge_falls_back_to_dlt_id() -> None:
    result = combine_tables(
        disposition="merge",
        existing=_t({"_dlt_id": "a", "value": 1}),
        incoming=_t({"_dlt_id": "a", "value": 2}),
        primary_key=None,
        fallback_key="_dlt_id",
    )
    assert result.to_pylist() == [{"_dlt_id": "a", "value": 2}]


def test_combine_tables_merge_promotes_new_column() -> None:
    # Incoming introduces a column the existing table lacks. It must survive the
    # merge (existing rows get null), not be dropped by first-row schema inference.
    existing = _t({"id": 1, "v": "a"}, {"id": 2, "v": "b"})
    incoming = _t({"id": 2, "v": "B", "tier": "gold"}, {"id": 3, "v": "c", "tier": "silver"})
    result = combine_tables(
        disposition="merge", existing=existing, incoming=incoming, primary_key=["id"]
    )
    assert "tier" in result.column_names
    by = {r["id"]: r for r in result.to_pylist()}
    assert by[1]["tier"] is None
    assert by[2]["tier"] == "gold"
    assert by[3] == {"id": 3, "v": "c", "tier": "silver"}


def test_combine_tables_merge_preserves_existing_column_type() -> None:
    # A bare from_pylist would re-infer `bal` from the values (e.g. decimal(4, 2)),
    # narrowing the existing decimal(12, 2) column and breaking the load.
    import decimal

    schema = pa.schema([("id", pa.int64()), ("bal", pa.decimal128(12, 2))])
    existing = pa.table({"id": [1], "bal": [decimal.Decimal("100.25")]}, schema=schema)
    incoming = pa.table({"id": [1], "bal": [decimal.Decimal("50.00")]}, schema=schema)
    result = combine_tables(
        disposition="merge", existing=existing, incoming=incoming, primary_key=["id"]
    )
    assert result.schema.field("bal").type == pa.decimal128(12, 2)
    assert result.to_pylist() == [{"id": 1, "bal": decimal.Decimal("50.00")}]


def test_combine_tables_insert_only_promotes_new_column() -> None:
    existing = _t({"id": 1, "v": "a"})
    incoming = _t({"id": 2, "v": "b", "tier": "gold"})
    result = combine_tables(
        disposition="insert-only", existing=existing, incoming=incoming, primary_key=["id"]
    )
    assert "tier" in result.column_names
    by = {r["id"]: r for r in result.to_pylist()}
    assert by[1]["tier"] is None
    assert by[2]["tier"] == "gold"


def test_combine_tables_rejects_unknown_disposition() -> None:
    with pytest.raises(ValueError, match="Unsupported write_disposition 'appned'"):
        combine_tables(
            disposition="appned",
            existing=_t({"id": 1}),
            incoming=_t({"id": 2}),
            primary_key=["id"],
        )
