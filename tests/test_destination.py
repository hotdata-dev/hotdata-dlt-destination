from hotdata_dlt_destination.destination import _augment_rows
from hotdata_dlt_destination.sql import append_sql, merge_sql, replace_from_staging_sql


def test_augment_rows_adds_metadata() -> None:
    batch_key, rows = _augment_rows(
        table_name="orders",
        items=[{"id": 1, "value": "a"}],
    )
    assert batch_key
    assert rows[0]["_hotdata_batch_key"] == batch_key
    assert "_hotdata_row_key" in rows[0]
    assert "_hotdata_loaded_at" in rows[0]


def test_sql_generation() -> None:
    append = append_sql(target="dlt.public.orders", staging="dlt.public._dlt_staging_orders")
    replace = replace_from_staging_sql(
        target="dlt.public.orders",
        staging="dlt.public._dlt_staging_orders",
    )
    merge = merge_sql(target="dlt.public.orders", staging="dlt.public._dlt_staging_orders")
    assert "INSERT INTO dlt.public.orders" in append[1]
    assert "DROP TABLE IF EXISTS dlt.public.orders" in replace[0]
    assert "DELETE FROM dlt.public.orders" in merge[1]
