# Architecture

## Goal

Provide a native `dlt` destination for Hotdata managed databases with explicit
contracts, predictable retries, full schema/state bookkeeping, nested-table
support, and DLT dataset readback.

## Components

- `factory.py`: the `hotdata` `Destination` and its capabilities
- `job_client.py`: `HotdataJobClient` (`JobClientBase` + `WithStateSync` + `WithSqlClient`) and `HotdataLoadJob`
- `configuration.py`: `HotdataCredentials` + `HotdataClientConfiguration` configspec
- `hotdata_client.py`: `ManagedDatabaseClient` subclass adding cross-run schema evolution over the `hotdata-framework` managed-database APIs
- `parquet.py`: parquet read/write for dlt load files and uploads
- `merge.py`: fallback row combining for `insert-only` and keyless `merge`
- `contracts.py`: deterministic database/schema/table mapping
- `sql_client.py`: DLT SQL-client adapter for `pipeline.dataset()` reads
- `ibis_backend.py`: optional live `dataset().ibis()` integration
- `errors.py`: transient vs terminal error mapping
- `config.py` + `cli.py`: env-configuration helper and the `hotdata-dlt-destination` console check
- `pipelines/`: example data pipelines

## Destination contract

`hotdata` is a native dlt destination implementing the load, state, and read
contracts via `JobClientBase`, `WithStateSync`, and `WithSqlClient`:

- Nested/child tables (`max_table_nesting`, default 1000) and `snake_case` identifiers.
- dlt internal columns (`_dlt_id`, `_dlt_load_id`) are preserved; no extra columns are added.
- Schema versioning (`_dlt_version`), load tracking (`_dlt_loads`), and pipeline state (`_dlt_pipeline_state`) are persisted as managed tables, so `get_stored_state` lets incremental sources resume across runs.
- Dataset readback: `pipeline.dataset()` queries loaded tables through Hotdata's server-side DataFusion engine and returns pandas, Arrow, rows, or ibis expressions.
- Write dispositions: `replace`, `append`, and `merge` from DLT resources. `merge` resolves to server-side `upsert` when a primary key is declared. `insert-only` is supported internally as a fallback combine strategy.
- `initialize_storage` declares the full table set (internal + user + schema tables) up front; if a later run needs a table the database lacks, `ensure_managed_database` declares it in place via `add_managed_table`. Existing tables (including dlt bookkeeping) are left untouched and no data is moved.

## Ingestion flow

1. `dlt` writes a parquet load file and hands its path plus the `table` schema to `HotdataLoadJob`.
2. Contract mapping converts table metadata into `{database}.{schema}.{table}` naming.
3. Write disposition comes from the dlt table schema, falling back to the destination default.
4. Managed database is resolved or created (`ensure_managed_database`, which declares any missing tables in place).
5. Load path uses Hotdata managed-table operations:
   - `replace`: upload the parquet batch and replace the target contents
   - `append`: upload the parquet batch and append it directly
   - keyed `merge`: upload the parquet batch and apply server-side `upsert`
   - keyed `merge` with `hard_delete`: split flagged rows into server-side delete and upsert operations
   - `insert-only` or keyless `merge`: fetch existing target rows, combine in Python, then replace the target
6. Schema version and load rows are written to the dlt bookkeeping tables on `update_stored_schema` / `complete_load`.

## Reliability model

- Retries: bounded retries with linear backoff
- Retryable classes: HTTP 408/409/425/429, HTTP 5xx, network timeout/connect failures
- Terminal classes: remaining HTTP/client errors, surfaced as `DestinationTerminalException`
- Row identity: dlt's `_dlt_id` (preserved on every row), or the resource `primary_key` when declared
- Parallelism: `sequential` by default because the managed-database load API currently serializes at catalog scope. Override `loader_parallelism_strategy` only when loads cannot contend for the same database.

## Known limitations

- A table's primary key and physical layout are fixed when the managed table is created. Changing DLT hints later does not rewrite the existing table definition.
- `delete-insert`, `scd2`, staging datasets, DDL transactions, and `merge_key` are not supported.
- `insert-only` and keyless `merge` still use a client-side combine and replace the target after reading current rows.
- Use `declared_tables` for multi-table pipelines so every table is declared up front. A table that is missing on a later run is added to the existing database in place via `add_managed_table`.
- `pipeline.dataset()` is destination readback. This package does not currently expose Hotdata managed tables as general-purpose DLT source resources.
