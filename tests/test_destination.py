from hotdata_dlt_destination.destination import _augment_rows


def test_augment_rows_adds_metadata() -> None:
    rows = _augment_rows(
        table_name="orders",
        items=[{"id": 1, "value": "a"}],
    )
    assert rows[0]["_hotdata_batch_key"]
    assert "_hotdata_row_key" in rows[0]
    assert "_hotdata_loaded_at" in rows[0]
