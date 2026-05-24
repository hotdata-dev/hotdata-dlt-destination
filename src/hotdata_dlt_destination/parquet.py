from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def read_parquet_rows(path: str | Path) -> list[dict[str, Any]]:
    table = pq.read_table(Path(path))
    return table.to_pylist()


def write_rows_parquet(rows: list[dict[str, Any]], path: str | Path) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, Path(path))


def write_table_parquet(table: pa.Table, path: str | Path) -> None:
    pq.write_table(table, Path(path))
