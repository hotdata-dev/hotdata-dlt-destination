"""Hotdata as a dlt *source*: managed tables and SQL queries as dlt resources.

```python
import dlt
from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.sources import hotdata_query, hotdata_table

orders = hotdata_table("orders", database_id="dbid...", primary_key="order_id")
# credentials and ids resolve from [sources.hotdata] when not passed

pipeline = dlt.pipeline("copy", destination="duckdb")
pipeline.run(orders)
```

Rows are yielded as Arrow batches, so the engine's own types reach the
destination without a Python round-trip.

SCOPE. These are **full reads**. There is deliberately no cursor/incremental
argument: an incremental read over a managed table needs a column that never
decreases for a row written later plus a tiebreaker when it repeats, and a table
has no such column unless something stamped one. Naming the wrong column fails by
silently skipping rows rather than by erroring, which is not a safe thing to ask
of a caller. Incremental reading is expected to arrive as a snapshot-based change
feed, where the position is supplied by the engine rather than chosen from the
schema.

Compose a full read with dlt's own `write_disposition` for the shapes this does
cover: replace-mode snapshots, one-off copies, and merge-on-key loads where the
caller bounds the query itself.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import dlt
from dlt.common.configuration import with_config
from dlt.common.configuration.specs import known_sections

from hotdata_dlt_destination.configuration import HotdataCredentials
from hotdata_dlt_destination.source_client import DEFAULT_BATCH_ROWS, HotdataSourceClient

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    import pyarrow as pa
    from dlt.extract.resource import DltResource

# Retry policy for the source's own reads. Mirrors the destination defaults: a
# load takes a catalog-level lock per database, so a concurrent writer can hold
# 409s for tens of seconds and the budget has to outlast that.
DEFAULT_MAX_RETRIES = 8
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_API_BASE_URL = "https://api.hotdata.dev"

_SQL_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _require_identifier(value: str, field: str) -> str:
    """Refuse anything that is not a plain identifier.

    These names are interpolated into SQL. Accepting a quoted or dotted string
    here would let the caller's `table=` compose the statement rather than name
    part of it, so the check is a refusal and not a normalisation. A table whose
    name needs quoting is reachable through `hotdata_query`, where the caller
    owns the whole statement and knows they do.
    """
    if not _SQL_IDENTIFIER.fullmatch(value or ""):
        raise ValueError(
            f"{field} {value!r} is not a plain SQL identifier. It is interpolated "
            "into the read, so it must name part of the statement rather than "
            "compose it; use hotdata_query() for anything this cannot express"
        )
    return value


def _select(
    *,
    schema: str,
    table: str,
    included_columns: Sequence[str] | None,
    limit: int | None,
) -> str:
    _require_identifier(schema, "schema")
    _require_identifier(table, "table")
    if isinstance(included_columns, str):
        # A bare string is a sequence of ONE-CHARACTER strings, and every one of
        # them is a valid identifier -- so without this the loop below passes and
        # the join produces `SELECT o, r, d, e, r, _, i, d`. Wrong SQL that runs.
        raise ValueError(
            f"included_columns {included_columns!r} is a bare string; it names the "
            "columns to read, so pass a list even for one column "
            f"([{included_columns!r}])"
        )
    for column in included_columns or ():
        _require_identifier(column, "included_columns")
    projection = ", ".join(included_columns) if included_columns else "*"
    tail = f" LIMIT {int(limit)}" if limit is not None else ""
    return f"SELECT {projection} FROM {schema}.{table}{tail}"


def _api_key(credentials: HotdataCredentials | None) -> str:
    """The key to read with, or an actionable error.

    Checked here rather than left to the first HTTP call so an unconfigured
    pipeline fails while it is being built, naming the setting to fix, instead of
    surfacing as a 401 from inside a load.
    """
    key = getattr(credentials, "api_key", None)
    if not key:
        raise ValueError(
            "hotdata source is missing an api_key. Pass "
            "credentials={'api_key': ...} or set it under [sources.hotdata.credentials] "
            "(SOURCES__HOTDATA__CREDENTIALS__API_KEY)"
        )
    return key


def _batches(
    *,
    sql: str,
    database_id: str,
    api_key: str,
    workspace_id: str,
    api_base_url: str,
    max_retries: int,
    retry_backoff_seconds: float,
    batch_rows: int,
) -> Iterator[pa.RecordBatch]:
    """Arrow batches for one read, with the client's lifetime bound to the scan.

    The client is opened here rather than by the caller so a resource that dlt
    never consumes opens no connection, and one that is consumed closes it even
    if the consumer stops early.
    """
    client = HotdataSourceClient(
        api_key=api_key,
        workspace_id=workspace_id,
        api_base_url=api_base_url,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    try:
        yield from client.read_arrow_batches(
            sql, database_id=database_id, batch_rows=batch_rows
        )
    finally:
        client.close()


@with_config(sections=(known_sections.SOURCES, "hotdata"))
def hotdata_table(
    table: str,
    *,
    schema: str = "public",
    included_columns: Sequence[str] | None = None,
    limit: int | None = None,
    database_id: str = dlt.config.value,
    workspace_id: str = dlt.config.value,
    credentials: HotdataCredentials = dlt.secrets.value,
    api_base_url: str = DEFAULT_API_BASE_URL,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    name: str | None = None,
    **resource_hints: Any,
) -> DltResource:
    """A managed table as a dlt resource.

    `database_id`, `workspace_id` and `credentials` resolve from
    `[sources.hotdata]` when not passed. The database read is an argument
    rather than shared with the destination because a pipeline commonly reads one
    database and writes another.

    `included_columns` restricts the SQL projection. It is deliberately NOT
    called `columns`: dlt already has a `columns` resource hint that describes the
    destination schema, and one name for two different things would mean a caller
    passing a schema hint silently got a projection built from its keys instead.

    `resource_hints` are passed to `dlt.resource` — `table_name`,
    `write_disposition`, `primary_key`, `columns`. Physical layout stays a
    destination concern: nothing here copies the source table's partitioning or
    sort order onto the destination, because the destination's layout is a
    property of how it will be queried, not of where the rows came from.
    """
    sql = _select(
        schema=schema, table=table, included_columns=included_columns, limit=limit
    )
    return dlt.resource(
        _batches,
        name=name or table,
        **resource_hints,
    )(
        sql=sql,
        database_id=database_id,
        api_key=_api_key(credentials),
        workspace_id=workspace_id,
        api_base_url=api_base_url,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        batch_rows=batch_rows,
    )


@with_config(sections=(known_sections.SOURCES, "hotdata"))
def hotdata_query(
    query: str,
    *,
    name: str,
    database_id: str = dlt.config.value,
    workspace_id: str = dlt.config.value,
    credentials: HotdataCredentials = dlt.secrets.value,
    api_base_url: str = DEFAULT_API_BASE_URL,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    **resource_hints: Any,
) -> DltResource:
    """A SQL query as a dlt resource.

    `name` is required: a query has no table to be named after, and a resource's
    name decides the destination table and the state it keeps, so defaulting it
    would silently share one identity between two different queries.

    `query` is run as written. It is caller-authored SQL and trusted as such —
    treat it the way you would treat code, not the way you would treat an API
    field. Do not build it from end-user input.
    """
    return dlt.resource(
        _batches,
        name=name,
        **resource_hints,
    )(
        sql=query,
        database_id=database_id,
        api_key=_api_key(credentials),
        workspace_id=workspace_id,
        api_base_url=api_base_url,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        batch_rows=batch_rows,
    )


__all__ = ["hotdata_query", "hotdata_table"]
