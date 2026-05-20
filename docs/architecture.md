# Architecture

## Goal

Provide a custom `dlt` destination for Hotdata managed databases with explicit contracts, predictable retries, and idempotent ingestion behavior.

## Components

- `destination.py`: custom destination entrypoint (`@dlt.destination`)
- `hotdata_client.py`: retry wrapper over `hotdata-runtime` managed-database APIs
- `parquet.py`: batch row serialization to parquet uploads
- `sql.py`: append/replace/merge SQL against qualified managed tables
- `contracts.py`: deterministic database/schema/table mapping
- `idempotency.py`: stable batch and row key generation
- `errors.py`: transient vs terminal error mapping
- `pipelines/`: example data pipelines

## Ingestion flow

1. `dlt` sends `(items, table)` into `hotdata_destination`.
2. Contract mapping converts table metadata into `{database}.{schema}.{table}` naming.
3. Each row receives `_hotdata_batch_key`, `_hotdata_row_key`, and `_hotdata_loaded_at`.
4. Rows are written to a temporary parquet file and uploaded via `upload_parquet`.
5. Managed database is resolved or created (`create_managed_database` when enabled).
6. Load path depends on write disposition:
   - `replace`: `load_managed_table` directly on the target table (`mode=replace`)
   - `append` / `merge`: `load_managed_table` into `_dlt_staging_{table}`, then SQL into target

## Reliability model

- Retries: bounded retries with linear backoff
- Retryable classes: HTTP 408/409/425/429, HTTP 5xx, network timeout/connect failures
- Terminal classes: remaining HTTP/client errors
- Idempotency: stable row and batch keys derived from canonical JSON

## Known limitations

- Managed-table loads currently support `mode=replace` only; append/merge use staging + SQL.
- `replace` replaces the full target table per batch (best for single-batch resources).
- This implementation is a custom destination callable, not a native dlt destination plugin package.
