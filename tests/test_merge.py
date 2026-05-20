from hotdata_dlt_destination.merge import (
    append_rows,
    combine_rows,
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


def test_append_rows_extends_existing() -> None:
    combined = append_rows([{"id": 1}], [{"id": 2}])
    assert combined == [{"id": 1}, {"id": 2}]


def test_merge_rows_updates_matching_primary_key() -> None:
    existing = [{"id": 1, "value": "old"}, {"id": 2, "value": "keep"}]
    incoming = [{"id": 1, "value": "new"}]
    merged = merge_rows(existing, incoming, primary_key=["id"])
    assert merged == [{"id": 1, "value": "new"}, {"id": 2, "value": "keep"}]


def test_combine_rows_replace_uses_incoming_only() -> None:
    combined = combine_rows(
        disposition="replace",
        existing=[{"id": 1}],
        incoming=[{"id": 2}],
        primary_key=["id"],
    )
    assert combined == [{"id": 2}]


def test_combine_rows_merge_falls_back_to_hotdata_row_key() -> None:
    combined = combine_rows(
        disposition="merge",
        existing=[{"_hotdata_row_key": "a", "value": 1}],
        incoming=[{"_hotdata_row_key": "a", "value": 2}],
        primary_key=None,
    )
    assert combined == [{"_hotdata_row_key": "a", "value": 2}]
