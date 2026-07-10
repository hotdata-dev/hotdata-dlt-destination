# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- MIT `LICENSE` file, `license`/`license-files` metadata in `pyproject.toml`, and a License section + badge in the README.

### Changed

- README reworked for open-source DX: badges, table of contents, a highlights summary, a requirements section, and `Development`/`Contributing` sections. Corrected the configuration table (retry defaults are `8` attempts / `1.5s`, not `5` / `1.0`) and documented the `api_base_url` and `loader_parallelism_strategy` options.
- Added `[project.urls]` (Homepage, Repository, Changelog, Issues) so the PyPI page links back to the project.

### Fixed

- Loading a `Decimal` (or wei) column without explicit precision hints no longer crashes. The destination's capabilities left `decimal_precision`/`wei_precision` unset, so dlt's normalize step raised `TypeError: 'NoneType' object is not subscriptable` in `get_py_arrow_numeric` while mapping the column to parquet. Capabilities now declare dlt's default numeric precision `(38, 9)` and wei precision `(78, 0)`, matching the Postgres numeric surface DataFusion presents.

## [0.7.1] - 2026-07-09

### Fixed

- A non-snake_case `database_name` (e.g. `my-hyphen-db`) no longer splits a pipeline's writes across two databases. Load jobs snake_cased the name when addressing the API — with `create_database_if_missing` that minted a twin database (`my_hyphen_db`) and loaded all data there — while schema/state/bookkeeping writes addressed the original verbatim, so reads against it failed with "declared but has no data". `database_name` and `schema` are opaque API addresses (a managed-database name or a `dbid...` id), not SQL identifiers, and now pass through verbatim on every path. Callers addressing by id were never affected.

## [0.7.0] - 2026-07-09

### Added

- Dataset read interface — data loaded with dlt can now be queried through the pipeline itself, the same read API as the `duckdb`/`postgres`/`bigquery` destinations. Queries run server-side on DataFusion (Postgres-compatible SQL) and return as Arrow/pandas. The client shipped in 0.6.1; these are its first release notes.
  - `pipeline.dataset().table("t").df()` / `.arrow()` / `.fetchall()`, raw SQL via `pipeline.dataset()("SELECT ...")`, and the fluent API (`select`/`where`/`order_by`/`limit`, aggregates, `row_counts()`).
  - New `HotdataSqlClient` + `HotdataCursor` over `pyarrow.Table` (`sql_client.py`); `sqlglot_dialect="postgres"` with identifier/literal escaping (`factory.py`); `HotdataClient.execute_sql`.
  - ibis expressions via `pipeline.dataset().table("t").to_ibis()` (built as ibis, compiled to SQL, executed through the sql_client). The live ibis backend (`dataset().ibis()`) is not supported — dlt maps it to a direct per-destination engine connection, which Hotdata's remote engine does not expose.
  - Querying a missing table raises dlt's `DatabaseUndefinedRelation` rather than a generic terminal error, so callers can tell "relation doesn't exist" from a real failure. `_make_database_exception` finds the engine's `"... not found"` even when it's nested under a generic `"400: Bad Request"` in the error's cause chain.

### Changed

- Cap `dlt>=1.28.1,<1.29` (was `>=1.28.1`): the read interface subclasses dlt internals (`SqlClientBase`, `DBApiCursorImpl`, `WithSqlClient`), which shift between dlt minor releases. (The `hotdata`/`hotdata-framework` caps were raised separately in 0.6.1.)

## [0.6.1] - 2026-07-08

### Fixed

- Default `loader_parallelism_strategy` is now `sequential` (was `table-sequential`). Managed-database loads lock at the catalog level, so parallel loads of *different* tables in the same database 409 each other — multi-table pipelines raced themselves and failed intermittently once the conflicts outlasted the retry budget. Override via `loader_parallelism_strategy` if concurrent table loads are ever wanted.
- Transient-retry defaults raised from 5 attempts x 1.0s to 8 attempts x 1.5s linear backoff (~42s budget) so a concurrent writer holding the catalog lock is outlasted instead of surfacing as `409: Conflict`.

### Changed

- Dependencies raised to `hotdata-framework>=0.6.1,<0.7` and `hotdata>=0.6.0,<0.7`: the framework carries the `X-Database-Id` scope on every result read (fixes repeat loads into an existing database failing with an opaque `400: Bad Request`) and preserves API error bodies; the 0.6.0 SDK exposes — and on `get_result_arrow` requires — the scope natively.
- `execute_sql` passes `x_database_id` natively on the Arrow fetch; the `X-Database-Id` default-header pinning (and the `fetch_table` pinning override) is removed — the framework now scopes every result read itself.

## [0.6.0] - 2026-06-30

### Fixed

- `merge`/`upsert`/`insert-only` loads no longer drop columns or narrow column types. `combine_tables` rebuilt the merged batch with `pa.Table.from_pylist`, which infers the schema from the first row — silently dropping columns present only in incoming rows (existing rows sort first and lack them) and re-inferring, often narrowing, column types. The latter could fail the load with a 409 type conflict (e.g. `decimal(12, 2)` → `decimal(5, 2)`). The merged batch is now built with the unified schema of the existing and incoming tables, so columns and types are preserved.

### Changed

- Schema evolution now declares missing tables in place via `add_managed_table` instead of recreating the managed database. Adding a table on a later run no longer snapshots, deletes, and reloads existing data — existing tables (including dlt bookkeeping) are left untouched. Requires `hotdata-framework>=0.6.0`.
- Clarified the write-modes documentation: dlt resources accept `append`/`replace`/`merge` only; `merge` is upsert-by-primary-key (what `upsert` resolves to), and `insert-only` is not selectable as a resource `write_disposition`.

## [0.5.0] - 2026-06-29

### Changed

- Bump the `hotdata-framework` floor to `>=0.5.0` and the `hotdata` SDK to `0.5.0`. Framework 0.5.0 is backward compatible (additive-only: an optional `format` field on `LoadManagedTableRequest` and an optional `format` parameter on `ResultsApi.get_result`), so no source changes were required; the full test suite passes.

## [0.4.2] - 2026-06-29

### Changed

- Bumped the `hotdata` dependency floor to `>=0.5.0` (was `>=0.4.1`).

## [0.4.1] - 2026-06-27

### Fixed

- **State sync was broken**: `normalize_identifier` stripped leading underscores, so the load-job path wrote the pipeline-state table as `dlt_pipeline_state` while `get_stored_state` read `_dlt_pipeline_state` — `get_stored_state` always returned `None` and incremental sources never resumed. Leading underscores are now preserved.
- **Nested/child tables were double-prefixed** (`orders__orders__items`): the table contract re-prepended `parent` to dlt's already-composed name. It now uses dlt's name as-is.
- **`update_stored_schema` crashed every real load** under dlt ≥1.28: the override didn't accept dlt's `force` keyword. It now accepts and forwards `force`.
- Added in-memory end-to-end tests (`tests/test_e2e_inmemory.py`) that run real dlt pipelines through the destination, plus contract regression tests, covering all three issues.

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

- Bump `hotdata-framework` to `>=0.4.1` — waits for result readiness on the synchronous query path, fixing merge/append loads and state reads.

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
