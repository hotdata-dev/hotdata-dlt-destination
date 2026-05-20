from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def write_rows_parquet(rows: list[dict[str, Any]], path: str | Path) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, Path(path))
