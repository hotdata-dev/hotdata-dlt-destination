from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems

from hotdata_dlt_destination.config import HotdataDestinationConfig
from hotdata_dlt_destination.contracts import TableContract
from hotdata_dlt_destination.errors import HotdataTerminalError
from hotdata_dlt_destination.hotdata_client import HotdataClient
from hotdata_dlt_destination.idempotency import compute_batch_key, compute_row_key
from hotdata_dlt_destination.merge import (
    combine_tables,
    resolve_primary_key,
    resolve_write_disposition,
)
from hotdata_dlt_destination.parquet import write_table_parquet


def _augment_table(
    *,
    table_name: str,
    items: TDataItems | str,
) -> pa.Table:
    # With loader_file_format="parquet" dlt always passes a file path; the
    # else branch is a defensive fallback for callers that pass dicts directly.
    if isinstance(items, str):
        table = pq.read_table(items)
    else:
        rows = [dict(item) for item in items]
        table = pa.Table.from_pylist(rows) if rows else pa.table({})

    # to_pylist() is called once to produce the row dicts needed for
    # deterministic hash computation. The table itself is not reconstructed
    # from dicts — metadata columns are appended directly instead.
    rows = table.to_pylist()
    batch_key = compute_batch_key(table_name, rows)
    loaded_at = datetime.now(UTC).isoformat()
    row_keys = [compute_row_key(table_name, row) for row in rows]

    n = len(table)
    table = table.append_column("_hotdata_batch_key", pa.array([batch_key] * n, type=pa.string()))
    table = table.append_column("_hotdata_row_key", pa.array(row_keys, type=pa.string()))
    table = table.append_column("_hotdata_loaded_at", pa.array([loaded_at] * n, type=pa.string()))
    return table


def _declared_tables(
    *,
    contract: TableContract,
    declared_tables: list[str] | None,
) -> list[str]:
    normalized_declared = TableContract.declared_table_names(
        database_name=contract.database_name,
        schema=contract.schema,
        table_names=declared_tables or [],
    )
    return sorted({*normalized_declared, contract.table_name})


@dlt.destination(
    batch_size=0,
    loader_file_format="parquet",
    loader_parallelism_strategy="table-sequential",
    name="hotdata",
    naming_convention="direct",
    max_table_nesting=0,
    skip_dlt_columns_and_tables=True,
)
def hotdata_destination(
    items: TDataItems | str,
    table: TTableSchema,
    api_key: str = dlt.secrets.value,
    workspace_id: str = dlt.secrets.value,
    api_base_url: str = "https://api.hotdata.dev",
    database_name: str = "dlt",
    schema: str = "public",
    write_disposition: str = "append",
    declared_tables: list[str] | None = None,
    create_database_if_missing: bool = True,
    max_retries: int = 5,
    retry_backoff_seconds: float = 1.0,
) -> None:
    config = HotdataDestinationConfig(
        api_key=api_key,
        workspace_id=workspace_id,
        api_base_url=api_base_url,
        database_name=database_name,
        schema=schema,
        write_disposition=write_disposition,
        declared_tables=tuple(declared_tables or ()),
        create_database_if_missing=create_database_if_missing,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    contract = TableContract.from_table_schema(
        table,
        database_name=config.database_name,
        schema=config.schema,
    )
    disposition = resolve_write_disposition(table, config.write_disposition)
    primary_key = resolve_primary_key(table)
    batch_table = _augment_table(table_name=contract.table_name, items=items)

    client = HotdataClient(
        api_key=config.api_key,
        workspace_id=config.workspace_id,
        api_base_url=config.api_base_url,
        max_retries=config.max_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )

    parquet_path: str | None = None
    try:
        client.ensure_managed_database(
            contract.database_name,
            schema=contract.schema,
            tables=_declared_tables(
                contract=contract,
                declared_tables=list(config.declared_tables),
            ),
            create_if_missing=config.create_database_if_missing,
        )

        table_to_load = batch_table
        if disposition != "replace":
            existing_table = client.fetch_table(
                database=contract.database_name,
                schema=contract.schema,
                table=contract.table_name,
            )
            table_to_load = combine_tables(
                disposition=disposition,
                existing=existing_table,
                incoming=batch_table,
                primary_key=primary_key,
            )

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as handle:
            parquet_path = handle.name
        write_table_parquet(table_to_load, parquet_path)

        upload_id = client.upload_parquet(parquet_path)
        client.load_managed_table(
            contract.database_name,
            contract.table_name,
            schema=contract.schema,
            upload_id=upload_id,
        )
    except HotdataTerminalError as error:
        raise DestinationTerminalException(str(error)) from error
    finally:
        if parquet_path is not None:
            try:
                os.unlink(parquet_path)
            except OSError:
                pass
        client.close()
