from __future__ import annotations

from typing import Any

import pyarrow as pa
from dlt.common.schema import TTableSchema

SUPPORTED_WRITE_DISPOSITIONS = frozenset({"replace", "append", "merge", "upsert"})


def resolve_write_disposition(table: TTableSchema, default: str) -> str:
    disposition = table.get("write_disposition") or default
    return str(disposition).lower()


def resolve_primary_key(table: TTableSchema) -> list[str] | None:
    primary_key = table.get("primary_key")
    if primary_key is None:
        return None
    if isinstance(primary_key, str):
        return [primary_key]
    return [str(key) for key in primary_key]


def row_key(row: dict[str, Any], keys: list[str]) -> tuple[Any, ...]:
    values = tuple(row.get(key) for key in keys)
    missing = [k for k, v in zip(keys, values, strict=True) if v is None]
    if missing:
        raise ValueError(
            f"Primary key field(s) {missing} are None or missing in row -- cannot merge"
        )
    return values


def merge_rows(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    primary_key: list[str],
) -> list[dict[str, Any]]:
    merged = list(existing)
    index = {row_key(row, primary_key): position for position, row in enumerate(merged)}
    for row in incoming:
        key = row_key(row, primary_key)
        if key in index:
            merged[index[key]] = row
        else:
            index[key] = len(merged)
            merged.append(row)
    return merged


def combine_tables(
    *,
    disposition: str,
    existing: pa.Table | None,
    incoming: pa.Table,
    primary_key: list[str] | None,
) -> pa.Table:
    """Arrow-native combine: avoids dict round-trip for replace and append."""
    if disposition == "replace" or existing is None or len(existing) == 0:
        return incoming
    if disposition == "append":
        # "permissive" fills missing columns with nulls so schema drift between
        # the existing table and the incoming batch doesn't raise an error.
        return pa.concat_tables([existing, incoming], promote_options="permissive")
    if disposition in ("merge", "upsert"):
        keys = primary_key or ["_hotdata_row_key"]
        merged = merge_rows(existing.to_pylist(), incoming.to_pylist(), primary_key=keys)
        return pa.Table.from_pylist(merged)
    raise ValueError(
        f"Unsupported write_disposition {disposition!r}. "
        f"Expected one of: {', '.join(sorted(SUPPORTED_WRITE_DISPOSITIONS))}"
    )
