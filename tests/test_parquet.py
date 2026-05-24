import pyarrow as pa
import pyarrow.parquet as pq

from hotdata_dlt_destination.parquet import write_table_parquet


def test_write_table_parquet_roundtrip(tmp_path) -> None:
    path = tmp_path / "rows.parquet"
    table = pa.table({"id": [1, 2], "name": ["Acme", "Globex"]})
    write_table_parquet(table, path)
    assert path.exists()
    assert path.stat().st_size > 0
    assert pq.read_table(path).to_pylist() == table.to_pylist()
