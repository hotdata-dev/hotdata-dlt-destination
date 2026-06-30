# Architecture

## Goal

Provide a native `dlt` destination for Hotdata managed databases with explicit contracts, predictable retries, full schema/state bookkeeping, and nested-table support.

## Components

- `factory.py`: the `hotdata` `Destination` and its capabilities
- `job_client.py`: `HotdataJobClient` (`JobClientBase` + `WithStateSync`) and `HotdataLoadJob`
- `configuration.py`: `HotdataCredentials` + `HotdataClientConfiguration` configspec
- `hotdata_client.py`: `ManagedDatabaseClient` subclass adding cross-run schema evolution over the `hotdata-framework` managed-database APIs
- `parquet.py`: parquet read/write for dlt load files and uploads
- `merge.py`: row combining for append/merge/upsert/insert-only dispositions (identity `fallback_key`, default `_dlt_id`)
- `contracts.py`: deterministic database/schema/table mapping
- `errors.py`: transient vs terminal error mapping
- `config.py` + `cli.py`: env-configuration helper and the `hotdata-dlt-destination` console check
- `pipelines/`: example data pipelines

## Destination contract

`hotdata` is a native dlt destination implementing the complete contract via `JobClientBase` and `WithStateSync`:

- Nested/child tables (`max_table_nesting`, default 1000) and `snake_case` identifiers.
- dlt internal columns (`_dlt_id`, `_dlt_load_id`) are preserved; no extra columns are added.
- Schema versioning (`_dlt_version`), load tracking (`_dlt_loads`), and pipeline state (`_dlt_pipeline_state`) are persisted as managed tables, so `get_stored_state` lets incremental sources resume across runs.
- Write dispositions: `replace`, `append`, `merge`, `upsert`, `insert-only`. Merge identity falls back to `_dlt_id` when no primary key is declared.
- `initialize_storage` declares the full table set (internal + user + schema tables) up front; if a later run needs a table the database lacks, `ensure_managed_database` declares it in place via `add_managed_table`. Existing tables (including dlt bookkeeping) are left untouched and no data is moved.

## Ingestion flow

1. `dlt` writes a parquet load file and hands its path plus the `table` schema to `HotdataLoadJob`.
2. Contract mapping converts table metadata into `{database}.{schema}.{table}` naming.
3. Write disposition comes from the dlt table schema, falling back to the destination default.
4. Managed database is resolved or created (`ensure_managed_database`, which declares any missing tables in place).
5. Load path uses only supported API operations:
   - `replace`: upload parquet batch and `load_managed_table(replace)` on the target
   - `append` / `merge` / `upsert` / `insert-only`: fetch existing target rows, combine in Python, then replace the target
6. Schema version and load rows are written to the dlt bookkeeping tables on `update_stored_schema` / `complete_load`.

## Reliability model

- Retries: bounded retries with linear backoff
- Retryable classes: HTTP 408/409/425/429, HTTP 5xx, network timeout/connect failures
- Terminal classes: remaining HTTP/client errors, surfaced as `DestinationTerminalException`
- Row identity: dlt's `_dlt_id` (preserved on every row), or the resource `primary_key` when declared
- Parallelism: `table-sequential` load jobs to avoid concurrent read-modify-write races

## Known limitations

- Managed-table loads only support `mode=replace`; append/merge/upsert/insert-only are emulated via read-modify-write.
- Use `declared_tables` for multi-table pipelines so every table is declared up front. A table that is missing on a later run is added to the existing database in place (via `add_managed_table`) — no recreate and no data movement.
- Read-modify-write reads the full target table on every non-replace batch.
