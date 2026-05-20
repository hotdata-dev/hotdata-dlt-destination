# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


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
