# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]



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
