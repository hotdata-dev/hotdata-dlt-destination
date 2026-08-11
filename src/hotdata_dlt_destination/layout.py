"""Resolve a managed table's storage layout from a dlt schema.

A hotdata table's partition and sort keys are fixed WHEN THE TABLE IS CREATED and
there is no alter path: no API call changes them, a delete tombstones the table
rather than freeing the name, and a fork refuses to copy into a layout-declaring
table. So a table created without the layout it wanted keeps that query profile
until someone recreates it and rewrites the data. Everything here is shaped by
that: declaring is the only chance, and a silently dropped hint is the failure
mode worth engineering against.

TWO WAYS IN, and the adapter is not a convenience.

  * dlt's standard per-column hints (``partition``, ``sort``) — the vocabulary a
    user already knows from BigQuery, Athena, ClickHouse and filesystem. Cheap to
    write, but a boolean carries no ORDER, no partition transform, and no sort
    direction or null placement. Key order is taken from the order the columns
    appear in the schema, which is the best available reading and still an
    implicit one.

  * ``hotdata_adapter`` — writes ``x-hotdata-partition`` / ``x-hotdata-sort``
    table hints carrying the full vocabulary explicitly, in the order given. This
    is the only way to say "sort by event_time, THEN tag_mac", and getting that
    order wrong is unrepairable, so anything that cares about order must use it.

The adapter wins where both are present, since it is the more specific statement.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dlt.common.schema.typing import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.destinations.utils import get_resource_for_adapter
from hotdata_framework import TablePartitionKey, TableSortKey

PARTITION_HINT = "x-hotdata-partition"
SORT_HINT = "x-hotdata-sort"

# The transforms the API accepts. `identity` partitions on the column's own value
# and is what an omitted transform means, which is why a plain boolean hint can
# express a partition at all but never a date-derived one.
PARTITION_TRANSFORMS = frozenset({"identity", "year", "month", "day", "hour"})
SORT_DIRECTIONS = frozenset({"asc", "desc"})
SORT_NULLS = frozenset({"first", "last"})


class LayoutError(ValueError):
    """A layout was declared that the API cannot accept, or cannot be read."""


def _partition_key(entry: Any) -> TablePartitionKey:
    """One partition key from a string, a pair, or a mapping.

    A bare column name means `identity` — the same default the API applies to an
    omitted transform, so the short form is not a special case.
    """
    if isinstance(entry, str):
        return TablePartitionKey(column=entry, transform="identity")
    if isinstance(entry, Mapping):
        column = entry.get("column")
        transform = str(entry.get("transform") or "identity").lower()
    elif isinstance(entry, Sequence) and len(entry) == 2:
        column, transform = entry[0], str(entry[1]).lower()
    else:
        raise LayoutError(
            f"partition entry must be a column name, a (column, transform) pair, or a "
            f"mapping with those keys — got {entry!r}"
        )
    if not column:
        raise LayoutError(f"partition entry {entry!r} has no column name")
    if transform not in PARTITION_TRANSFORMS:
        raise LayoutError(
            f"partition transform {transform!r} is not one of "
            f"{sorted(PARTITION_TRANSFORMS)} (column {column!r})"
        )
    return TablePartitionKey(column=str(column), transform=transform)


def _sort_key(entry: Any) -> TableSortKey:
    """One sort key. Direction and nulls are left unset when not given, so the
    server applies its own defaults rather than this package guessing them."""
    if isinstance(entry, str):
        return TableSortKey(column=entry, direction=None, nulls=None)
    if isinstance(entry, Mapping):
        column = entry.get("column")
        direction = entry.get("direction")
        nulls = entry.get("nulls")
    elif isinstance(entry, Sequence) and 1 <= len(entry) <= 3:
        column = entry[0]
        direction = entry[1] if len(entry) > 1 else None
        nulls = entry[2] if len(entry) > 2 else None
    else:
        raise LayoutError(
            f"sort entry must be a column name, a (column[, direction[, nulls]]) "
            f"tuple, or a mapping with those keys — got {entry!r}"
        )
    if not column:
        raise LayoutError(f"sort entry {entry!r} has no column name")
    if direction is not None:
        direction = str(direction).lower()
        if direction not in SORT_DIRECTIONS:
            raise LayoutError(
                f"sort direction {direction!r} is not one of {sorted(SORT_DIRECTIONS)} "
                f"(column {column!r})"
            )
    if nulls is not None:
        nulls = str(nulls).lower()
        if nulls not in SORT_NULLS:
            raise LayoutError(
                f"sort nulls {nulls!r} is not one of {sorted(SORT_NULLS)} "
                f"(column {column!r})"
            )
    return TableSortKey(column=str(column), direction=direction, nulls=nulls)


def hotdata_adapter(
    data: Any,
    partition_by: Iterable[Any] | None = None,
    sorted_by: Iterable[Any] | None = None,
) -> Any:
    """Declare a table's storage layout explicitly, in order.

    The dlt-native way to carry destination-specific hints, matching how the
    Iceberg and BigQuery adapters work: values land in the resource's
    ``additional_table_hints`` under an ``x-hotdata-*`` key and are read back when
    the table is declared.

    Use this rather than per-column ``partition`` / ``sort`` hints whenever KEY
    ORDER, a partition transform, a sort direction or null placement matters — a
    per-column boolean cannot express any of them, and a layout cannot be
    corrected after the table exists.

        hotdata_adapter(
            my_resource,
            partition_by=[("event_date", "identity")],
            sorted_by=["event_time", ("tag_mac", "asc", "last")],
        )

    Entries are validated here rather than at load time, so a bad transform is a
    definition-time error instead of a request the server rejects after the
    pipeline has already extracted and normalised.
    """
    resource = get_resource_for_adapter(data)
    hints: dict[str, Any] = {}
    if partition_by is not None:
        keys = [_partition_key(e) for e in partition_by]
        if not keys:
            raise LayoutError("partition_by was given but empty; omit it instead")
        hints[PARTITION_HINT] = keys
    if sorted_by is not None:
        keys = [_sort_key(e) for e in sorted_by]
        if not keys:
            raise LayoutError("sorted_by was given but empty; omit it instead")
        hints[SORT_HINT] = keys
    if hints:
        resource.apply_hints(additional_table_hints=hints)
    return resource


def resolve_partition_by(table: TTableSchema) -> list[TablePartitionKey]:
    """The table's partition keys, adapter first, then per-column hints."""
    hinted = table.get(PARTITION_HINT)
    if hinted:
        return [_partition_key(e) for e in hinted]
    # Plain `partition` column hints. Order follows the schema's column order,
    # which is the only ordering available here — use the adapter when it matters.
    return [
        TablePartitionKey(column=name, transform="identity")
        for name in get_columns_names_with_prop(table, "partition")
    ]


def resolve_sorted_by(table: TTableSchema) -> list[TableSortKey]:
    """The table's sort keys, adapter first, then per-column hints."""
    hinted = table.get(SORT_HINT)
    if hinted:
        return [_sort_key(e) for e in hinted]
    return [
        TableSortKey(column=name, direction=None, nulls=None)
        for name in get_columns_names_with_prop(table, "sort")
    ]


def missing_layout_columns(table: TTableSchema) -> list[str]:
    """Layout columns that are not in the table's schema.

    A partition on a column the table does not have is rejected by the server, but
    only once the request is made — by which point a pipeline has extracted,
    normalised and uploaded. Reporting it from `verify_schema` turns that into a
    definition error before any of that work happens.
    """
    columns = set((table.get("columns") or {}).keys())
    named = [k.column for k in resolve_partition_by(table)]
    named += [k.column for k in resolve_sorted_by(table)]
    return sorted({c for c in named if c not in columns})


def declares_layout(table: TTableSchema) -> bool:
    """Whether this table asks for any layout at all.

    The reason this matters beyond bookkeeping: the FIRST load into a
    layout-declaring table must use `replace`. Establishing the layout takes
    several commits server-side, and replace is what makes that sequence
    retry-safe. A table declared with a layout therefore needs seeding even when
    its write disposition is append, or its first load is rejected outright.
    """
    return bool(resolve_partition_by(table) or resolve_sorted_by(table))

