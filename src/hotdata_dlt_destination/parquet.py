from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_table_parquet(table: pa.Table, path: str | Path) -> None:
    pq.write_table(table, Path(path))
