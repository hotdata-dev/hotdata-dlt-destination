# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New native `hotdata` destination — a dlt `JobClientBase` + `WithStateSync` plugin — exported as `from hotdata_dlt_destination import hotdata`. It implements dlt's complete destination contract:
  - Pipeline **state sync** (`get_stored_state`) so incremental sources restore their state from the managed database across runs.
  - Schema versioning (`_dlt_version`) and load tracking (`_dlt_loads`); dlt internal columns (`_dlt_id`, `_dlt_load_id`) are preserved.
  - Nested/child tables (`max_table_nesting`, default 1000) and `snake_case` identifiers.
  - `insert-only` write disposition, in addition to `replace`/`append`/`merge`/`upsert`.
  - Cross-run schema evolution: when an existing managed database is missing a declared table, it is recreated with the union of existing and required tables.
- `configuration.py` (`HotdataCredentials`, `HotdataClientConfiguration` configspec), `factory.py` (`hotdata` `Destination`), and `job_client.py` (`HotdataJobClient`, `HotdataLoadJob`).
- Tests: `tests/test_factory.py` (capabilities) and `tests/test_job_client.py` (load job + state sync against a fake client); `insert-only` and `_dlt_id`-fallback cases in `tests/test_merge.py`.

### Removed

- **Breaking:** removed the lightweight `hotdata_destination` `@dlt.destination` sink and the `_hotdata_batch_key` / `_hotdata_row_key` / `_hotdata_loaded_at` idempotency columns it added (`destination.py`, `idempotency.py`). Use `hotdata`, which relies on dlt's native `_dlt_id` for row identity. Migrate `destination=hotdata_destination(...)` → `destination=hotdata(...)` (pass `credentials=HotdataCredentials(api_key=..., workspace_id=...)` or set the env vars).

### Changed

- `hotdata_client.HotdataClient` is now a `hotdata_framework.managed_client.ManagedDatabaseClient` subclass that adds the cross-run union-recreate `ensure_managed_database` (was a thin re-export).
- `merge.combine_tables` gained a `fallback_key` parameter (default `_dlt_id`); `insert-only` added to `SUPPORTED_WRITE_DISPOSITIONS`.

## [0.4.0] - 2026-06-22

### Changed

- Upgrade `hotdata` SDK to `>=0.4.1` (was `>=0.2.4`) and `hotdata-runtime` to `>=0.3.0` (was `>=0.2.0`); refresh `uv.lock`.
- Raise the `dlt` floor to `>=1.28.1` (was `>=1.26.0`); the full test suite stays green.
- `hotdata_client.HotdataClient` is now a thin re-export of `hotdata_runtime.managed_client.ManagedDatabaseClient`; the managed-database client logic (bounded retries, Arrow result fetching, managed-table lifecycle) is now owned and tested upstream in `hotdata-runtime`.
- `errors.py` re-exports the typed error hierarchy from `hotdata_runtime.errors` (`HotdataError`, `HotdataTransientError`, `HotdataTerminalError`, `classify_sdk_error`); `HotdataDestinationError` is kept as a backward-compatible alias of `HotdataError`.
- The destination load/write path now maps `HotdataTransientError` (transient failures that survived the runtime's bounded retries) to dlt's `DestinationTransientException`, so dlt's retry layer can re-attempt the load; terminal failures still map to `DestinationTerminalException`.
- Test fixtures updated for the hotdata 0.4.1 `QueryResponse` model (now-required `preview_row_count` and `truncated` fields).

### Added

- Strict `[tool.mypy]` configuration and a `mypy>=1.11` dev dependency (not wired into CI).
- Expanded ruff lint `select` (`W`, `N`, `C4`, `DTZ`, `T20`, `RET`, `SIM`, `RUF`) with targeted per-file ignores; applied `ruff check --fix` and `ruff format` across `src`, `scripts`, and `tests`.

## [0.3.4] - 2026-05-27

### Changed

- Release 0.3.4

## [0.3.3] - 2026-05-24

### Added

- Arrow-native write path: `pq.read_table()` delivers a `pa.Table` directly; metadata columns appended with `append_column()` — no `from_pylist()` reconstruction. `to_pylist()` called once only for SHA256 idempotency key computation.
- Arrow-native read path: `fetch_table()` returns a `pa.Table` via `hotdata.arrow.ResultsApi.get_result_arrow()` (Arrow IPC) instead of JSON rows.
- `_query_database_scoped()`: passes `X-Database-Id` header so SQL resolves to `"default".<schema>.<table>` inside a managed database's catalog.
- `_wait_result_ready()`: polls `ResultsApi` until a result leaves `processing` state before calling `get_result_arrow`.
- `combine_tables()` in `merge.py`: Arrow-native combine for replace, append (`pa.concat_tables`), and merge/upsert dispositions.
- `pa.concat_tables` uses `promote_options="permissive"` so schema drift between batches fills missing columns with nulls.
- Comprehensive `combine_tables` test coverage: replace, append with schema drift, merge by primary key, upsert, `_hotdata_row_key` fallback, `None`/empty existing.
- `scripts/load_test.py`: end-to-end load test — creates N managed databases with synthetic Parquet data, uploads, loads, queries via Arrow IPC, and reports per-phase timing stats (mean, p50, p95, min, max, rows/s).
- `uv lock` now runs automatically during `scripts/release.sh prepare` to keep the lockfile in sync with version bumps.

### Changed

- `fetch_table_rows()` delegates to `fetch_table()` and calls `.to_pylist()` on the result.
- `write_table_parquet()` is now the sole Parquet write helper in `parquet.py`.
- README rewritten for a developer audience: quickstart, configuration table, write modes, multi-table setup, and demo walkthrough.

### Removed

- `read_parquet_rows()` and `write_rows_parquet()` from `parquet.py` (replaced by Arrow-native path).
- `combine_rows()` and `append_rows()` from `merge.py` (replaced by `combine_tables()`).

### Fixed

- Managed-database queries now pass the `X-Database-Id` header; previously all queries returned 400 errors for non-replace dispositions.
- `get_result_arrow` no longer raises `ResultNotReadyError` — `_wait_result_ready()` polls until the result is ready before fetching.

## [0.3.2] - 2026-05-24

### Added

- FRED macro-economic indicators demo pipeline (`hotdata-dlt-demo`) downloading 9 series directly from `fred.stlouisfed.org` and loading a long-format raw table and a wide monthly table into a Hotdata managed database.
- `pandas>=2.0` declared as an explicit direct dependency.

### Changed

- `_request_with_retry`: collapsed two identical `raise mapped_error from error` branches into one.
- `_classify_error`: simplified to `classify_sdk_error(error.__cause__ or error)`, removing double-wrap.
- Retry backoff capped at `_MAX_BACKOFF_SECONDS` (30 s) to prevent unbounded waits.
- SQL table identifiers double-quoted to prevent injection.
- `parquet_path` initialised to `None`; cleanup guard uses `is not None` check.
- `_augment_rows` returns `list[dict]` directly (removed unused tuple wrapper).
- Runbook updated to reference `hotdata-dlt-demo` only; stale pipeline entries removed.

### Removed

- `basic_pipeline.py`, `incremental_pipeline.py`, `linear_pipeline.py` and their script entrypoints.
- `run-basic.sh`, `run-incremental.sh`, `run-linear.sh` helper scripts.
- `test_e2e_linear_hotdata.py` (depended on deleted Linear pipeline).

### Fixed

- `errors.py`: removed access to `error.body` (not present on all SDK errors).
- `merge.py`: `row_key()` now raises `ValueError` on `None` primary-key fields instead of silently producing wrong keys.
- `config.py`: `max_retries` and `retry_backoff_seconds` validated at parse time.
- `cli.py`: workspace ID truncated before printing to avoid leaking full value.


## [0.3.1] - 2026-05-24

### Changed

- Require `hotdata-runtime>=0.2.0`.
- Pass `description` as a keyword argument to `create_managed_database` to match the updated `hotdata-runtime` 0.2.0 API.

## [0.3.0] - 2026-05-20

### Changed

- Replace SQL staging append/merge with read-modify-write using supported API operations only (`SELECT`, `upload_parquet`, `load_managed_table(replace)`).
- Switch to dlt parquet file mode (`batch_size=0`) instead of re-encoding typed-jsonl batches.
- Require `hotdata>=0.2.2` for reliable `ApiClient.close()` lifecycle support.

### Added

- Per-resource `write_disposition` and `primary_key` from dlt table schema.
- `declared_tables` destination config (and `HOTDATA_DECLARED_TABLES` env var) for multi-table managed databases.
- `DestinationTerminalException` mapping for non-retryable Hotdata errors.
- Synced-table guard before `SELECT` to avoid 500s on never-loaded managed tables.

### Removed

- `sql.py` DML staging path (Hotdata query API does not support DML/DDL on managed tables).

## [0.2.0] - 2026-05-20

### Changed

- Load exclusively through Hotdata managed databases (parquet upload + `load_managed_table`).
- Replace legacy datasets API usage with `hotdata-runtime>=0.1.1` and `hotdata>=0.2.0`.
- Rename destination config from `dataset_prefix` to `database_name` with optional auto-create.

### Added

- Parquet batch serialization, staging tables for append/merge, and release automation scripts.
