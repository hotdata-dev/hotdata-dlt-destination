# Architecture

## Goal

Provide a custom `dlt` destination for Hotdata managed databases with explicit contracts, predictable retries, and idempotent ingestion behavior.

## Components

- `destination.py`: custom destination entrypoint (`@dlt.destination`)
- `hotdata_client.py`: retry wrapper over `hotdata-runtime` managed-database APIs
- `parquet.py`: parquet read/write for dlt load files and uploads
- `merge.py`: read-modify-write row combining for append/merge dispositions
- `contracts.py`: deterministic database/schema/table mapping
- `idempotency.py`: stable batch and row key generation
- `errors.py`: transient vs terminal error mapping
- `pipelines/`: example data pipelines

## Ingestion flow

1. `dlt` sends a parquet load file path and `table` schema into `hotdata_destination`.
2. Contract mapping converts table metadata into `{database}.{schema}.{table}` naming.
3. Each row receives `_hotdata_batch_key`, `_hotdata_row_key`, and `_hotdata_loaded_at`.
4. Write disposition comes from the dlt table schema, falling back to the destination default.
5. Managed database is resolved or created (`create_managed_database` when enabled).
6. Load path uses only supported API operations:
   - `replace`: upload parquet batch and `load_managed_table(replace)` on the target
   - `append` / `merge`: `SELECT *` existing target rows, combine in Python, then replace the target

## Reliability model

- Retries: bounded retries with linear backoff
- Retryable classes: HTTP 408/409/425/429, HTTP 5xx, network timeout/connect failures
- Terminal classes: remaining HTTP/client errors, surfaced as `DestinationTerminalException`
- Idempotency: stable row and batch keys derived from canonical JSON
- Parallelism: `table-sequential` load jobs to avoid concurrent read-modify-write races

## Known limitations

- Managed-table loads only support `mode=replace`; append/merge are emulated via read-modify-write.
- Tables must be declared when the managed database is created; use `declared_tables` for multi-table pipelines.
- Read-modify-write reads the full target table on every append/merge batch.
- This implementation is a custom destination callable, not a native dlt destination plugin package.
