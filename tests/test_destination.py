from hotdata_dlt_destination.destination import _augment_table


def test_augment_table_adds_metadata() -> None:
    table = _augment_table(
        table_name="orders",
        items=[{"id": 1, "value": "a"}],
    )
    row = table.to_pylist()[0]
    assert row["_hotdata_batch_key"]
    assert "_hotdata_row_key" in row
    assert "_hotdata_loaded_at" in row
