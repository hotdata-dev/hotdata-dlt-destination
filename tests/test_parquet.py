from hotdata_dlt_destination.parquet import read_parquet_rows, write_rows_parquet


def test_write_rows_parquet_roundtrip(tmp_path) -> None:
    path = tmp_path / "rows.parquet"
    rows = [{"id": 1, "name": "Acme"}, {"id": 2, "name": "Globex"}]
    write_rows_parquet(rows, path)
    assert path.exists()
    assert path.stat().st_size > 0
    assert read_parquet_rows(path) == rows
