# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

<!--
Changelog style — terse "bullet + brief why":
- One bullet per user-facing change. State what changed; add a short clause of
  rationale only where it matters (breaking changes, gotchas, version bumps).
- Keep symbol/API names and version requirements; drop mechanism and backstory
  (the git history holds those). These notes are for a reader deciding whether
  to upgrade.
- Prefer one bullet with semicolon-joined clauses over nested sub-bullets.
- Group under ### Added / Changed / Fixed / Removed. Mark breaking changes
  **Breaking:**. See the released entries below for the target density.
-->

## [0.13.2] - 2026-08-18

### Changed

- Docs only: README feature support now covers data types (`Decimal` defaults to
  `(38, 9)`, `wei` stored as `(78, 0)`) and marks the `delete-insert`/`scd2` merge
  strategies as coming soon; clarified that the SQL/ibis read interface is
  destination readback, not a general-purpose dlt source. `docs/architecture.md`,
  `docs/runbook.md`, and `docs/sql-client-spec.md` refreshed to match shipped
  behavior. No package or API change.

## [0.13.1] - 2026-08-11

### Changed

- Condensed the changelog to a terse "bullet + brief why" house style, and pinned
  that style with a guide comment under `[Unreleased]` that `release.sh prepare`
  preserves across releases (`update_changelog.py`); `RELEASING.md` documents the
  convention. Repo docs/tooling only — no package or API change.

## [0.13.0] - 2026-08-12

### Added

- Storage layout from dlt hints — a table's partition and sort keys are declared
  when the destination creates it, resolved from dlt's `partition`/`sort` column
  hints or the new `hotdata_adapter` (needed for key order, partition transform,
  and sort direction/nulls, which the boolean hints can't carry). Layout is fixed
  at creation with no alter path: existing tables are never modified, a layout
  naming an unknown column fails in `verify_schema` before any upload, and
  layout-declaring tables are seeded with a zero-row `replace` when created.

## [0.12.0] - 2026-08-11

### Changed

- Require `hotdata>=0.9.0,<0.10`, `hotdata-framework>=0.12.0,<0.13`, and (for the
  `ibis` extra) `hotdata-ibis>=0.5.0,<0.6`. No code change — hotdata 0.9.0's
  breaking removals don't reach this package. Unblocks downstream adoption of
  `hotdata-framework` 0.12, which also first exposes table storage layout (#63).

## [0.11.0] - 2026-07-24

### Added

- `database_id` param (`hotdata(database_id=...)`) / `HOTDATA_DATABASE_ID` env /
  `[destination.hotdata] database_id` config — target an existing managed database
  by id. On a first run with no id, one is created from its `database_name` label
  and the new id is logged so it can be pinned to reuse across runs.

### Changed

- **Breaking:** managed databases are addressed strictly by id, never by name —
  Hotdata names are non-unique display labels, so name-matching could silently
  read, write, or drop the wrong database. An existing database is bound by id via
  `GET /databases/{id}`; with no id and `create_database_if_missing`, one is
  created and used for the run. To load into the same database across runs, pin
  its `database_id`. Requires `hotdata-framework>=0.9.0`.
- Create/upload/query-scoped API keys (forbidden from reading `/databases`) can
  now bootstrap a database: the no-`database_id` create path issues no read, so
  the key only makes the create it is permitted to make.
- **Breaking:** `workspace_id` moved out of `HotdataCredentials` to a top-level
  `hotdata(workspace_id=...)` param / config field, matching the SDK's
  `Configuration(api_key=, workspace_id=)` shape. It has no env fallback
  (`HOTDATA_WORKSPACE` is no longer read here). Passing it inside `credentials={...}`
  is deprecated (warns); `HotdataCredentials(workspace_id=...)` now raises `TypeError`.

### Fixed

- The API key resolves from `HOTDATA_API_KEY` per the README quickstart, instead
  of leaving `credentials.api_key` unset and failing with an opaque `NoneType`
  error. Missing `api_key`/`workspace_id` now raises a clear
  `ConfigurationValueError` at setup naming the missing field.


## [0.10.0] - 2026-07-20

### Changed

- Keyed loads (`delete`/`update`/`upsert`) send the resolved `primary_key` as a
  per-load key, so the merge matches even when the table wasn't created with a
  declared key. Requires `hotdata-framework>=0.8.0`.

### Added

- `merge` loads honour dlt's `hard_delete` column hint: flagged rows are deleted
  by key (server `delete` mode) while the rest upsert. Requires a `primary_key`.
  Follows dlt's rule — a boolean column deletes on `True`, other types on non-null.

## [0.9.5] - 2026-07-16

### Changed

- Raised the `hotdata-framework` floor to 0.7.3 so the destination can't pair with
  the pre-streaming upload path (0.7.3 streams parquet uploads under a bounded
  memory budget). No code changes.

## [0.9.4] - 2026-07-16

### Fixed

- Truncate now empties replace tables with a zero-row `mode="replace"` load instead
  of delete + re-declare. The API's delete-table endpoint tombstones the table (it
  can never be re-declared or reloaded), so 0.9.3's truncate permanently broke every
  replace table it touched. **Do not use 0.9.3.**

## [0.9.3] - 2026-07-16

### Fixed

- Multi-file replace data loss: tables split across multiple parquet files loaded
  every file with `mode="replace"`, so each file wiped the previous and only the
  last survived. Replace tables are now truncated once per load package in
  `initialize_storage`, and every file job appends.

## [0.9.2] - 2026-07-15

### Changed

- Bumped `hotdata-framework` to 0.7.1, which streams large Parquet uploads
  chunk-by-chunk via the presigned session API. Eliminates client-side OOM on
  large tables.

## [0.9.1] - 2026-07-15

### Added

- `HotdataLoadJob.run()` logs each Parquet file as it begins loading:
  `load: <table> <- <file> (<N> rows)`, making the loading phase visible in dlt
  stdout instead of a silent gap.

## [0.9.0] - 2026-07-15

### Changed

- Loads use the server's native load modes instead of a client-side
  read-modify-write: `append`/`replace` upload directly, and `merge` with a
  `primary_key` maps to native `upsert`. `insert-only` and keyless `merge` still
  combine client-side and replace.
- Require `hotdata-ibis>=0.3.1` for the `[ibis]` extra (0.3.0 pinned `hotdata<0.7`,
  incompatible with the 0.7 client this release needs).

### Fixed

- `merge`/`upsert` dedupe by the resource's `primary_key` instead of the row-unique
  `_dlt_id`, which produced cross-run duplicates.
- Read-modify-write combines no longer fail with an Arrow `string_view` vs `string`
  type error.

## [0.8.0] - 2026-07-13

### Added

- Live ibis backend — `pipeline.dataset().ibis()` returns a live `ibis.hotdata`
  connection to the remote engine (supersedes the 0.7.0 unsupported note).
  Expressions and raw SQL run server-side, returning Arrow/pandas, alongside the
  existing `.to_ibis()` compile-to-SQL path. Behind the `[ibis]` extra; requires
  `hotdata-ibis` on ibis 12 and `ibis-framework>=12,<13`. dlt's
  `create_ibis_backend` has no third-party hook, so the destination wraps it
  (`ibis_backend.py`) — Hotdata gets the out-of-tree backend, other destinations
  pass through. Adds `HotdataClient.resolve_managed_database` and a
  `hotdata-dlt-ibis-demo` script.

## [0.7.2] - 2026-07-09

### Added

- MIT `LICENSE` file, `license`/`license-files` metadata in `pyproject.toml`, and
  a License section + badge in the README.

### Changed

- README reworked for open-source DX (badges, table of contents, highlights,
  requirements, Development/Contributing sections). Corrected the retry defaults
  (8 attempts / 1.5s) and documented `api_base_url` and `loader_parallelism_strategy`.
- Added `[project.urls]` (Homepage, Repository, Changelog, Issues) so the PyPI page
  links back to the project.

### Fixed

- Loading a `Decimal` (or wei) column without precision hints no longer crashes —
  capabilities now declare dlt's default numeric `(38, 9)` and wei `(78, 0)`
  precision, fixing a `TypeError` in dlt's normalize step.

## [0.7.1] - 2026-07-09

### Fixed

- A non-snake_case `database_name` (e.g. `my-hyphen-db`) no longer splits a
  pipeline's writes across two databases. Load jobs snake_cased the name (minting a
  twin `my_hyphen_db` under `create_database_if_missing`) while schema/state writes
  used it verbatim. `database_name`/`schema` are opaque API addresses and now pass
  through verbatim on every path. Addressing by id was never affected.

## [0.7.0] - 2026-07-09

### Added

- Dataset read interface — data loaded with dlt can be queried through the pipeline,
  the same read API as `duckdb`/`postgres`/`bigquery`. Queries run server-side on
  DataFusion (Postgres-compatible SQL), returning Arrow/pandas. Includes
  `pipeline.dataset().table("t").df()/.arrow()/.fetchall()`, raw SQL via
  `pipeline.dataset()("SELECT ...")`, the fluent API (`select`/`where`/`order_by`/
  `limit`, aggregates, `row_counts()`), and ibis via `.to_ibis()` (compiled to SQL).
  New `HotdataSqlClient`/`HotdataCursor` (`sql_client.py`), `sqlglot_dialect="postgres"`,
  `HotdataClient.execute_sql`. A missing table raises dlt's `DatabaseUndefinedRelation`
  so callers can tell it from a real failure. (The live `dataset().ibis()` backend is
  not yet supported.)

### Changed

- Cap `dlt>=1.28.1,<1.29` (was `>=1.28.1`): the read interface subclasses dlt
  internals (`SqlClientBase`, `DBApiCursorImpl`, `WithSqlClient`) that shift between
  minor releases.

## [0.6.1] - 2026-07-08

### Fixed

- Default `loader_parallelism_strategy` is now `sequential` (was `table-sequential`).
  Managed-database loads lock at the catalog level, so parallel loads of different
  tables in the same database 409'd each other and raced. Override if concurrent
  table loads are ever wanted.
- Transient-retry defaults raised from 5×1.0s to 8×1.5s linear backoff (~42s budget)
  to outlast a concurrent writer holding the catalog lock.

### Changed

- Dependencies raised to `hotdata-framework>=0.6.1,<0.7` and `hotdata>=0.6.0,<0.7`,
  which carry the `X-Database-Id` scope on every result read (fixes repeat loads into
  an existing database failing with an opaque `400: Bad Request`).
- `execute_sql` passes `x_database_id` natively; the manual `X-Database-Id` header
  pinning is removed — the framework scopes every result read itself.

## [0.6.0] - 2026-06-30

### Fixed

- `merge`/`upsert`/`insert-only` loads no longer drop columns or narrow types.
  `combine_tables` rebuilt the batch with `pa.Table.from_pylist`, inferring the
  schema from the first row — dropping columns present only in incoming rows and
  narrowing types (which could 409). The merged batch now uses the unified schema of
  the existing and incoming tables.

### Changed

- Schema evolution declares missing tables in place via `add_managed_table` instead
  of recreating the managed database, so adding a table on a later run no longer
  snapshots, deletes, and reloads existing data. Requires `hotdata-framework>=0.6.0`.
- Clarified write-modes docs: resources accept `append`/`replace`/`merge` only;
  `merge` is upsert-by-primary-key, and `insert-only` isn't selectable as a resource
  `write_disposition`.

## [0.5.0] - 2026-06-29

### Changed

- Bump the `hotdata-framework` floor to `>=0.5.0` and the `hotdata` SDK to `0.5.0`.
  Framework 0.5.0 is additive-only (an optional `format` field/param), so no source
  changes were required; the suite passes.

## [0.4.2] - 2026-06-29

### Changed

- Bumped the `hotdata` dependency floor to `>=0.5.0` (was `>=0.4.1`).

## [0.4.1] - 2026-06-27

### Fixed

- **State sync was broken:** `normalize_identifier` stripped leading underscores, so
  the load path wrote `dlt_pipeline_state` while `get_stored_state` read
  `_dlt_pipeline_state` — state always returned `None` and incremental sources never
  resumed. Leading underscores are now preserved.
- **Nested/child tables were double-prefixed** (`orders__orders__items`): the contract
  re-prepended `parent` to dlt's already-composed name. It now uses dlt's name as-is.
- **`update_stored_schema` crashed every real load** under dlt ≥1.28: the override
  didn't accept dlt's `force` keyword. It now accepts and forwards it.
- Added in-memory end-to-end tests (`tests/test_e2e_inmemory.py`) plus contract
  regression tests covering all three issues.

### Added

- New native `hotdata` destination — a dlt `JobClientBase` + `WithStateSync` plugin,
  exported as `from hotdata_dlt_destination import hotdata`. Implements dlt's full
  contract: state sync (`get_stored_state`), schema versioning (`_dlt_version`) and
  load tracking (`_dlt_loads`), nested/child tables (`max_table_nesting`),
  `snake_case` identifiers, and `insert-only` alongside `replace`/`append`/`merge`/
  `upsert`. Cross-run schema evolution recreates a database with the union of existing
  and required tables when a declared table is missing.
- `configuration.py`, `factory.py`, and `job_client.py` (`HotdataJobClient`,
  `HotdataLoadJob`), plus tests (`test_factory.py`, `test_job_client.py`, merge cases).

### Removed

- **Breaking:** removed the lightweight `hotdata_destination` `@dlt.destination` sink
  and its `_hotdata_batch_key`/`_hotdata_row_key`/`_hotdata_loaded_at` idempotency
  columns. Use `hotdata` (relies on dlt's native `_dlt_id`); migrate
  `destination=hotdata_destination(...)` → `destination=hotdata(...)`.

### Changed

- `HotdataClient` is now a `ManagedDatabaseClient` subclass adding the union-recreate
  `ensure_managed_database`.
- `merge.combine_tables` gained a `fallback_key` parameter (default `_dlt_id`);
  `insert-only` added to `SUPPORTED_WRITE_DISPOSITIONS`.

## [0.4.0] - 2026-06-22

### Changed

- Upgrade `hotdata` SDK to `>=0.4.1` and `hotdata-runtime` to `>=0.3.0`; refresh
  `uv.lock`. Raise the `dlt` floor to `>=1.28.1`; the suite stays green.
- `HotdataClient` is now a thin re-export of
  `hotdata_runtime.managed_client.ManagedDatabaseClient`; the managed-database client
  logic (bounded retries, Arrow fetching, table lifecycle) is now owned upstream.
- `errors.py` re-exports the typed hierarchy from `hotdata_runtime.errors`;
  `HotdataDestinationError` kept as a backward-compatible alias of `HotdataError`.
- The load path maps `HotdataTransientError` to dlt's `DestinationTransientException`
  (so dlt can retry); terminal failures still map to `DestinationTerminalException`.
- Test fixtures updated for hotdata 0.4.1's `QueryResponse` (`preview_row_count`,
  `truncated`).

### Added

- Strict `[tool.mypy]` configuration and a `mypy>=1.11` dev dependency (not wired into
  CI).
- Expanded ruff lint `select` (`W`, `N`, `C4`, `DTZ`, `T20`, `RET`, `SIM`, `RUF`) with
  per-file ignores; applied `ruff check --fix` and `ruff format` across the tree.

## [0.3.4] - 2026-05-27

### Changed

- Bump `hotdata-framework` to `>=0.4.1` — waits for result readiness on the
  synchronous query path, fixing merge/append loads and state reads.

## [0.3.3] - 2026-05-24

### Added

- Arrow-native write path: `pq.read_table()` delivers a `pa.Table` directly; metadata
  columns appended via `append_column()` (no `from_pylist()` reconstruction).
  `to_pylist()` is called once, only for the SHA256 idempotency key.
- Arrow-native read path: `fetch_table()` returns a `pa.Table` via `get_result_arrow()`
  (Arrow IPC) instead of JSON rows.
- `_query_database_scoped()` passes `X-Database-Id` so SQL resolves inside a managed
  database's catalog; `_wait_result_ready()` polls until a result leaves `processing`.
- `combine_tables()` in `merge.py`: Arrow-native combine for replace/append
  (`pa.concat_tables`, `promote_options="permissive"` for schema drift) and
  merge/upsert, with comprehensive test coverage.
- `scripts/load_test.py`: end-to-end load test reporting per-phase timing stats.
  `uv lock` now runs during `scripts/release.sh prepare`.

### Changed

- `fetch_table_rows()` delegates to `fetch_table()` + `.to_pylist()`;
  `write_table_parquet()` is the sole Parquet write helper. README rewritten for a
  developer audience.

### Removed

- `read_parquet_rows()`/`write_rows_parquet()` from `parquet.py` and
  `combine_rows()`/`append_rows()` from `merge.py` (replaced by the Arrow-native path).

### Fixed

- Managed-database queries now pass `X-Database-Id` (previously all non-replace queries
  400'd); `get_result_arrow` no longer raises `ResultNotReadyError`
  (`_wait_result_ready()` polls first).

## [0.3.2] - 2026-05-24

### Added

- FRED macro-economic indicators demo pipeline (`hotdata-dlt-demo`) loading a
  long-format raw table and a wide monthly table into a managed database. `pandas>=2.0`
  declared as a direct dependency.

### Changed

- Retry path simplified (`_request_with_retry`, `_classify_error`) with backoff capped
  at 30s; SQL identifiers double-quoted against injection; `parquet_path` cleanup
  guarded with `is not None`; `_augment_rows` returns `list[dict]` directly. Runbook
  updated to `hotdata-dlt-demo` only.

### Removed

- `basic`/`incremental`/`linear` pipelines, their script entrypoints, the run helper
  scripts, and `test_e2e_linear_hotdata.py`.

### Fixed

- `errors.py` no longer accesses `error.body` (not present on all SDK errors);
  `merge.row_key()` raises on `None` primary-key fields; config retry values validated
  at parse time; workspace ID truncated before printing.


## [0.3.1] - 2026-05-24

### Changed

- Require `hotdata-runtime>=0.2.0`.
- Pass `description` as a keyword argument to `create_managed_database` to match the
  updated `hotdata-runtime` 0.2.0 API.

## [0.3.0] - 2026-05-20

### Changed

- Replace SQL staging append/merge with read-modify-write using supported API
  operations only (`SELECT`, `upload_parquet`, `load_managed_table(replace)`).
- Switch to dlt parquet file mode (`batch_size=0`) instead of re-encoding typed-jsonl
  batches.
- Require `hotdata>=0.2.2` for reliable `ApiClient.close()` lifecycle support.

### Added

- Per-resource `write_disposition` and `primary_key` from dlt table schema.
- `declared_tables` destination config (and `HOTDATA_DECLARED_TABLES` env var) for
  multi-table managed databases.
- `DestinationTerminalException` mapping for non-retryable Hotdata errors.
- Synced-table guard before `SELECT` to avoid 500s on never-loaded managed tables.

### Removed

- `sql.py` DML staging path (Hotdata query API does not support DML/DDL on managed
  tables).

## [0.2.0] - 2026-05-20

### Changed

- Load exclusively through Hotdata managed databases (parquet upload +
  `load_managed_table`).
- Replace legacy datasets API usage with `hotdata-runtime>=0.1.1` and `hotdata>=0.2.0`.
- Rename destination config from `dataset_prefix` to `database_name` with optional
  auto-create.

### Added

- Parquet batch serialization, staging tables for append/merge, and release automation
  scripts.
