# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-05-20

### Changed

- Load exclusively through Hotdata managed databases (parquet upload + `load_managed_table`).
- Replace legacy datasets API usage with `hotdata-runtime>=0.1.1` and `hotdata>=0.2.0`.
- Rename destination config from `dataset_prefix` to `database_name` with optional auto-create.

### Added

- Parquet batch serialization, staging tables for append/merge, and release automation scripts.
