from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from dlt.common.schema import TTableSchema

SUPPORTED_WRITE_DISPOSITIONS = frozenset({"replace", "append", "merge", "upsert", "insert-only"})


def resolve_write_disposition(table: TTableSchema, default: str) -> str:
    disposition = table.get("write_disposition") or default
    return str(disposition).lower()


def resolve_primary_key(table: TTableSchema) -> list[str] | None:
    # dlt encodes primary_key as per-column hints, not a top-level table key —
    # read those too (else merge falls back to _dlt_id and duplicates across runs).
    primary_key = table.get("primary_key")
    if primary_key is not None:
        if isinstance(primary_key, str):
            return [primary_key]
        return [str(key) for key in primary_key]
    columns = table.get("columns") or {}
    hinted = [name for name, col in columns.items() if col.get("primary_key")]
    return hinted or None


def resolve_merge_strategy(table: TTableSchema) -> str | None:
    """The table's merge strategy (dlt's ``x-merge-strategy``), or None.

    Read separately from ``write_disposition`` (always ``"merge"``) to tell an
    ``upsert`` from an ``insert-only`` load.
    """
    from dlt.common.schema.utils import get_merge_strategy

    name = table.get("name")
    if not name:
        return None
    return get_merge_strategy({name: table}, name)


def resolve_hard_delete_column(table: TTableSchema) -> str | None:
    """The column dlt marks with the ``hard_delete`` hint, or None.

    On a merge load, rows flagged in this column are deletes rather than upserts.
    """
    if not table.get("columns"):
        return None
    from dlt.common.schema.utils import get_first_column_name_with_prop

    return get_first_column_name_with_prop(table, "hard_delete")


def _hard_delete_mask(table: pa.Table, column: str) -> pa.ChunkedArray:
    # dlt's rule: a bool hard-delete column deletes the row on True; a column of
    # any other type deletes on NOT NULL. A null in a bool column is not a delete.
    col = table.column(column)
    if pa.types.is_boolean(col.type):
        return pc.fill_null(pc.equal(col, pa.scalar(True)), False)
    return pc.is_valid(col)


def split_hard_delete(table: pa.Table, column: str) -> tuple[pa.Table, pa.Table]:
    """Split ``table`` into (rows to upsert, rows to delete) by the hard-delete column."""
    deleted = _hard_delete_mask(table, column)
    return table.filter(pc.invert(deleted)), table.filter(deleted)


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


def _string_view_to_string(table: pa.Table) -> pa.Table:
    """Cast Arrow view/large string+binary columns to plain string/binary.

    The server returns strings as Utf8View, which pyarrow won't concat/unify
    with the plain Utf8 of an incoming parquet batch.
    """
    new_fields = []
    changed = False
    for field in table.schema:
        t = field.type
        if pa.types.is_string_view(t) or pa.types.is_large_string(t):
            new_fields.append(field.with_type(pa.string()))
            changed = True
        elif pa.types.is_binary_view(t) or pa.types.is_large_binary(t):
            new_fields.append(field.with_type(pa.binary()))
            changed = True
        else:
            new_fields.append(field)
    return table.cast(pa.schema(new_fields)) if changed else table


def combine_tables(
    *,
    disposition: str,
    existing: pa.Table | None,
    incoming: pa.Table,
    primary_key: list[str] | None,
    fallback_key: str = "_dlt_id",
    hard_delete_column: str | None = None,
) -> pa.Table:
    """Arrow-native combine: avoids dict round-trip for replace and append.

    ``fallback_key`` is the row-identity column used for merge/upsert/insert-only
    when no ``primary_key`` is declared. dlt's ``_dlt_id`` is preserved on every
    row, so it is the default. ``hard_delete_column`` (merge only) marks rows to
    remove by key instead of upsert — used on the client-side fallback path.
    """
    if disposition == "replace" or existing is None or len(existing) == 0:
        if hard_delete_column is not None and hard_delete_column in incoming.column_names:
            # No existing rows to delete; keep only the non-deleted incoming rows.
            return split_hard_delete(incoming, hard_delete_column)[0]
        return incoming
    # normalise Arrow string encodings so the two sides concat/unify
    existing = _string_view_to_string(existing)
    incoming = _string_view_to_string(incoming)
    if disposition == "append":
        # "permissive" fills missing columns with nulls so schema drift between
        # the existing table and the incoming batch doesn't raise an error.
        return pa.concat_tables([existing, incoming], promote_options="permissive")
    keys = primary_key or [fallback_key]
    if disposition in ("merge", "upsert"):
        if hard_delete_column is not None and hard_delete_column in incoming.column_names:
            to_upsert, to_delete = split_hard_delete(incoming, hard_delete_column)
            deleted_keys = {row_key(r, keys) for r in to_delete.to_pylist()}
            merged = merge_rows(existing.to_pylist(), to_upsert.to_pylist(), primary_key=keys)
            merged = [r for r in merged if row_key(r, keys) not in deleted_keys]
        else:
            merged = merge_rows(existing.to_pylist(), incoming.to_pylist(), primary_key=keys)
        # Build with the unified schema of both inputs. A bare ``pa.Table.from_pylist``
        # infers the schema from the first row's keys and values, which silently drops
        # incoming-only columns (existing rows sort first and lack them) and re-infers
        # -- often narrowing -- column types. Passing the unified schema preserves every
        # column and the widest compatible type, so the merged batch never loses a
        # column or narrows a type relative to the existing table.
        schema = pa.unify_schemas(
            [existing.schema, incoming.schema], promote_options="permissive"
        )
        return pa.Table.from_pylist(merged, schema=schema)
    if disposition == "insert-only":
        existing_keys = {row_key(row, keys) for row in existing.to_pylist()}
        new_rows = [r for r in incoming.to_pylist() if row_key(r, keys) not in existing_keys]
        if not new_rows:
            return existing
        # Preserve the incoming schema for the new rows (same reasoning as above),
        # then let permissive concat reconcile it with the existing table.
        new_table = pa.Table.from_pylist(new_rows, schema=incoming.schema)
        return pa.concat_tables([existing, new_table], promote_options="permissive")
    raise ValueError(
        f"Unsupported write_disposition {disposition!r}. "
        f"Expected one of: {', '.join(sorted(SUPPORTED_WRITE_DISPOSITIONS))}"
    )
