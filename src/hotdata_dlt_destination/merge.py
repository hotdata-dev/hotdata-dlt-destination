from __future__ import annotations

from typing import Any

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
    return tuple(row.get(key) for key in keys)


def append_rows(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [*existing, *incoming]


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


def combine_rows(
    *,
    disposition: str,
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    primary_key: list[str] | None,
) -> list[dict[str, Any]]:
    if disposition == "replace":
        return incoming
    if disposition in ("merge", "upsert"):
        keys = primary_key or ["_hotdata_row_key"]
        return merge_rows(existing, incoming, primary_key=keys)
    if disposition == "append":
        return append_rows(existing, incoming)
    raise ValueError(
        f"Unsupported write_disposition {disposition!r}. "
        f"Expected one of: {', '.join(sorted(SUPPORTED_WRITE_DISPOSITIONS))}"
    )
