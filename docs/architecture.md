# Architecture

## Goal

Provide a custom `dlt` destination for Hotdata managed databases with explicit contracts, predictable retries, and idempotent ingestion behavior.

## Components

Two destinations share the lower-level modules:

- `destination.py`: lightweight sink entrypoint (`@dlt.destination`), exported as `hotdata_destination`
- `factory.py` + `job_client.py` + `configuration.py`: the native `hotdata` destination (`JobClientBase` + `WithStateSync`)
- `hotdata_client.py`: `ManagedDatabaseClient` subclass adding cross-run schema evolution over the `hotdata-framework` managed-database APIs
- `parquet.py`: parquet read/write for dlt load files and uploads
- `merge.py`: row combining for append/merge/upsert/insert-only dispositions (configurable identity `fallback_key`)
- `contracts.py`: deterministic database/schema/table mapping
- `idempotency.py`: stable batch and row key generation (sink only)
- `errors.py`: transient vs terminal error mapping
- `pipelines/`: example data pipelines

## Full destination (`hotdata`)

A native dlt destination implementing the complete contract via `JobClientBase` and `WithStateSync`:

- Nested/child tables (`max_table_nesting`, default 1000) and `snake_case` identifiers.
- dlt internal columns (`_dlt_id`, `_dlt_load_id`) are preserved; no `_hotdata_*` columns are added.
- Schema versioning (`_dlt_version`), load tracking (`_dlt_loads`), and pipeline state (`_dlt_pipeline_state`) are persisted as managed tables, so `get_stored_state` lets incremental sources resume across runs.
- Write dispositions: `replace`, `append`, `merge`, `upsert`, `insert-only`. Merge identity falls back to `_dlt_id` when no primary key is declared.
- `initialize_storage` declares the full table set up front; if a later run needs a table the database lacks, `ensure_managed_database` recreates the database with the union of existing and required tables. Because managed-database tables can only be declared at creation time, the recreate snapshots every existing table first and reloads it afterward, so no data (including dlt bookkeeping) is lost.

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
