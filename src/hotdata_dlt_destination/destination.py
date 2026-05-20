from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime
from typing import Any

import dlt
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems

from hotdata_dlt_destination.config import HotdataDestinationConfig
from hotdata_dlt_destination.contracts import TableContract
from hotdata_dlt_destination.hotdata_client import HotdataClient
from hotdata_dlt_destination.idempotency import compute_batch_key, compute_row_key
from hotdata_dlt_destination.parquet import write_rows_parquet
from hotdata_dlt_destination.sql import append_sql, merge_sql


def _augment_rows(
    *,
    table_name: str,
    items: TDataItems,
) -> tuple[str, list[dict[str, Any]]]:
    rows = [dict(item) for item in items]
    batch_key = compute_batch_key(table_name, rows)
    loaded_at = datetime.now(UTC).isoformat()
    augmented_rows = [
        {
            **row,
            "_hotdata_batch_key": batch_key,
            "_hotdata_row_key": compute_row_key(table_name, row),
            "_hotdata_loaded_at": loaded_at,
        }
        for row in rows
    ]
    return batch_key, augmented_rows


@dlt.destination(
    batch_size=500,
    loader_file_format="typed-jsonl",
    name="hotdata",
    naming_convention="direct",
    max_table_nesting=0,
    skip_dlt_columns_and_tables=True,
)
def hotdata_destination(
    items: TDataItems,
    table: TTableSchema,
    api_key: str = dlt.secrets.value,
    workspace_id: str = dlt.secrets.value,
    api_base_url: str = "https://api.hotdata.dev",
    database_name: str = "dlt",
    schema: str = "public",
    write_disposition: str = "append",
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
        create_database_if_missing=create_database_if_missing,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )
    contract = TableContract.from_table_schema(
        table,
        database_name=config.database_name,
        schema=config.schema,
    )
    _, rows = _augment_rows(table_name=contract.table_name, items=items)

    client = HotdataClient(
        api_key=config.api_key,
        workspace_id=config.workspace_id,
        api_base_url=config.api_base_url,
        max_retries=config.max_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )

    parquet_path = ""
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as handle:
            parquet_path = handle.name
        write_rows_parquet(rows, parquet_path)

        client.ensure_managed_database(
            contract.database_name,
            schema=contract.schema,
            tables=[contract.table_name, contract.staging_table_name],
            create_if_missing=config.create_database_if_missing,
        )
        upload_id = client.upload_parquet(parquet_path)

        if config.write_disposition == "replace":
            client.load_managed_table(
                contract.database_name,
                contract.table_name,
                schema=contract.schema,
                upload_id=upload_id,
            )
            return

        client.load_managed_table(
            contract.database_name,
            contract.staging_table_name,
            schema=contract.schema,
            upload_id=upload_id,
        )

        if config.write_disposition in ("merge", "upsert"):
            statements = merge_sql(
                target=contract.qualified_target,
                staging=contract.qualified_staging,
            )
        else:
            statements = append_sql(
                target=contract.qualified_target,
                staging=contract.qualified_staging,
            )

        for statement in statements:
            client.execute_sql(statement)
    finally:
        if parquet_path:
            os.unlink(parquet_path)
        client.close()
