# hotdata-dlt-destination

[![PyPI version](https://img.shields.io/pypi/v/hotdata-dlt-destination.svg)](https://pypi.org/project/hotdata-dlt-destination/)
[![Python versions](https://img.shields.io/pypi/pyversions/hotdata-dlt-destination.svg)](https://pypi.org/project/hotdata-dlt-destination/)
[![CI](https://github.com/hotdata-dev/hotdata-dlt-destination/actions/workflows/ci.yml/badge.svg)](https://github.com/hotdata-dev/hotdata-dlt-destination/actions/workflows/ci.yml)
[![dlt](https://img.shields.io/badge/dlt-1.28-blue.svg)](https://dlthub.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Load data into [Hotdata](https://hotdata.dev) managed databases using [dlt](https://dlthub.com) — then read it back through the same pipeline.

dlt handles extraction, schema inference, and batching. This package is a **native dlt destination** (`JobClientBase` + `WithStateSync` + `WithSqlClient`) for the Hotdata side: it uploads each batch as Parquet, registers it with your managed database, syncs pipeline state so incremental sources resume, and exposes a server-side read API.

**Highlights**

- **Native destination** — nested/child tables, dlt internal columns (`_dlt_id`, `_dlt_load_id`), schema versioning, and load tracking, all preserved.
- **Incremental resume** — pipeline state is persisted in the managed database, so incremental sources pick up where they left off across runs.
- **Read your data back** — query loaded tables through `pipeline.dataset()` (pandas / Arrow / fluent SQL / ibis), server-side on Apache DataFusion. No Hotdata-specific code.
- **In-place schema evolution** — new tables and columns are added to an existing database without recreating it or moving data.
- **Append / replace / merge (upsert)** — standard dlt write dispositions, plus an `insert-only` combine.

## Contents

- [Requirements](#requirements)
- [Install](#install)
- [Quickstart](#quickstart)
- [Read your data back](#read-your-data-back)
- [Feature support](#feature-support)
- [Configuration](#configuration)
- [Write modes](#write-modes)
- [Multiple tables](#multiple-tables)
- [Verify a load](#verify-a-load)
- [Demo pipeline](#demo-pipeline)
- [How it works](#how-it-works)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)
- [Resources](#resources)

## Requirements

- Python **3.11+**
- A [Hotdata](https://hotdata.dev) workspace, an API key, and its workspace ID. Grab both from your Hotdata dashboard, or with the [Hotdata CLI](https://github.com/hotdata-dev/sdk-python).

## Install

```bash
pip install hotdata-dlt-destination
# or
uv add hotdata-dlt-destination

# with the live ibis backend (dataset().ibis()):
pip install "hotdata-dlt-destination[ibis]"
```

## Quickstart

```python
import dlt
from hotdata_dlt_destination import hotdata

@dlt.resource(name="orders", write_disposition="append")
def orders_resource():
    yield [
        {"id": 1, "customer": "Alice", "total": 99.00},
        {"id": 2, "customer": "Bob",   "total": 49.50},
    ]

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=hotdata(
        workspace_id="your_workspace_id",
        database_name="sales",
        declared_tables=["orders"],
    ),
)

pipeline.run(orders_resource())
```

Set your API key in the environment before running:

```bash
export HOTDATA_API_KEY=your_api_key
```

The API key is a secret, so it's read from the environment (or a dlt secrets provider). The workspace ID is routing, not a secret — pass it as the `workspace_id=` parameter (switching workspaces is a one-line change).

That's it. On first run, the `sales` managed database is created automatically and the `orders` table is loaded.

`hotdata` supports nested/child tables, preserves dlt's internal columns (`_dlt_id`, `_dlt_load_id`), and persists schema-version, load, and pipeline-state tables in the managed database so **incremental sources resume correctly across runs**. If an existing managed database is missing a declared table on a later run, the table is added to it in place; existing tables and their data are left untouched.

## Read your data back

The **same `pipeline` object** that writes can read, through dlt's standard [dataset interface](https://dlthub.com/docs/general-usage/dataset-access/dataset) — no Hotdata-specific code, no database IDs, no hand-written SQL:

```python
ds = pipeline.dataset()

ds.table("orders").df()      # whole table -> pandas.DataFrame
ds.table("orders").arrow()   # -> pyarrow.Table

# raw SQL
ds("SELECT customer, sum(total) AS spend FROM orders GROUP BY customer").df()

# fluent
ds.table("orders").select("id", "total").where("total > 50").order_by("total").limit(10).df()
```

Queries run server-side on Hotdata's Apache DataFusion engine (Postgres-compatible SQL). It's the same read API you'd use with the `duckdb`, `postgres`, or `bigquery` destinations — enabled because `hotdata` advertises dlt's SQL-client interface (`WithSqlClient`).

You can also author queries with **ibis**, two ways. `dataset().table("orders").to_ibis()` gives an ibis table that dlt compiles to SQL and runs through the same client. `dataset().ibis()` returns a live `ibis.hotdata` backend — ibis expressions and raw SQL run server-side and come back as pandas/Arrow. The live backend needs the `[ibis]` extra (see [Install](#install)).

## Feature support

Where `hotdata` stands against the [dlt destination capability spec](https://dlthub.com/docs/dlt-ecosystem/destinations/). ✅ supported · ⚠️ supported with caveats · ❌ not supported.

### Write dispositions

| Disposition | Support | Notes |
|-------------|:-------:|-------|
| `append`    | ✅ | Native `append` — the batch is added; existing data untouched |
| `replace`   | ✅ | Native `replace` — table contents fully replaced |
| `merge`     | ✅ | Native `upsert` by `primary_key` (updates matches, inserts the rest). Without a `primary_key` it falls back to a client-side combine — see merge strategies below |

> **Keys are fixed at table creation.** A table's key is declared the first time it's created in the managed database. Changing a resource's `primary_key` on a later run does **not** update the server-side key. A table that already exists *without* a key (e.g. created before this feature) can't gain one in place — the connector detects the missing key and falls back to a client-side combine for merges, so loads don't break.

### Merge strategies

| Strategy        | Support | Notes |
|-----------------|:-------:|-------|
| `upsert`        | ✅ | Default. Dedupes by `primary_key`, falling back to dlt's `_dlt_id` |
| `insert-only`   | ✅ | Inserts rows whose key isn't already present; never updates existing rows |
| `delete-insert` | ❌ | Not supported |
| `scd2`          | ❌ | Not supported |

### Replace strategies

| Strategy              | Support | Notes |
|-----------------------|:-------:|-------|
| `truncate-and-insert` | ✅ | |
| `insert-from-staging` | ❌ | No staging dataset |
| `staging-optimized`   | ❌ | No staging dataset |

### Keys & column hints

| Feature       | Support | Notes |
|---------------|:-------:|-------|
| `primary_key` | ✅ | Drives merge/upsert and insert-only de-duplication |
| `merge_key`   | ❌ | Use `primary_key` |
| `hard_delete` | ❌ | Deletes are not propagated |
| `dedup_sort`  | ❌ | |

### Loader file formats

| Format          | Support | Notes |
|-----------------|:-------:|-------|
| `parquet`       | ✅ | Preferred and only loader format |
| `jsonl`         | ❌ | |
| `insert_values` | ❌ | |
| `csv`           | ❌ | |

### Structure & lifecycle

| Feature | Support | Notes |
|---------|:-------:|-------|
| Nested / child tables | ✅ | Up to `max_table_nesting` (default `1000`), e.g. `orders__items` |
| dlt internal columns (`_dlt_id`, `_dlt_load_id`) | ✅ | Preserved, never stripped |
| dlt system tables (`_dlt_loads`, `_dlt_version`) | ✅ | Persisted in the managed database |
| Pipeline state sync (`WithStateSync`) | ✅ | Incremental sources resume across runs |
| Dataset read API (`pipeline.dataset()`) | ✅ | Read loaded data as pandas / arrow / fluent SQL, server-side on DataFusion — see [Read your data back](#read-your-data-back) |
| ibis expressions (`.table("t").to_ibis()`) | ✅ | Built as ibis, compiled to SQL, executed via the sql_client |
| Live ibis backend (`dataset().ibis()`) | ✅ | Live `ibis.hotdata` connection to the remote engine; needs the `[ibis]` extra |
| New columns | ✅ | Permissive column promotion on append/merge |
| New tables | ✅ | A table missing on a later run is declared in place on the existing database — no recreate, no data movement |
| Multiple tables per pipeline | ✅ | Pass every table name via `declared_tables` |

### Staging, transactions & identifiers

| Feature | Support | Notes |
|---------|:-------:|-------|
| Filesystem / remote staging | ❌ | Parquet is uploaded directly to Hotdata |
| Staging dataset | ❌ | |
| DDL transactions | ❌ | |
| Case-sensitive identifiers | ❌ | `snake_case`, case-insensitive; identifiers up to 255 chars |

## Configuration

| Parameter | Env variable | Default | Description |
|-----------|-------------|---------|-------------|
| `api_key` | `HOTDATA_API_KEY` | required | Your Hotdata API key (a secret; passed via `credentials=` or the env var) |
| `workspace_id` | — | required | Your Hotdata workspace ID — pass as the `hotdata(workspace_id=...)` param (no env var) |
| `database_name` | `HOTDATA_DATABASE` | `dlt` | Managed database to load into |
| `schema` | `HOTDATA_SCHEMA` | `public` | Schema within the managed database |
| `write_disposition` | `HOTDATA_WRITE_DISPOSITION` | `append` | Default write mode (see [Write modes](#write-modes)) |
| `declared_tables` | `HOTDATA_DECLARED_TABLES` | — | All table names the pipeline will write (required for multi-table pipelines — see [Multiple tables](#multiple-tables)) |
| `create_database_if_missing` | `HOTDATA_CREATE_DATABASE_IF_MISSING` | `True` | Create the managed database if it doesn't exist yet |
| `api_base_url` | `HOTDATA_API_BASE_URL` | `https://api.hotdata.dev` | Hotdata API endpoint |
| `max_retries` | `HOTDATA_MAX_RETRIES` | `8` | How many times to retry a failed request |
| `retry_backoff_seconds` | `HOTDATA_RETRY_BACKOFF_SECONDS` | `1.5` | Initial wait between retries (grows linearly with each attempt) |

Pass these as keyword arguments to `hotdata(...)`. The `api_key` is the exception — being a secret, it's read from `HOTDATA_API_KEY` (or supplied via `credentials=`, e.g. `hotdata(credentials={"api_key": "..."}, ...)`); `workspace_id` has no environment variable. `hotdata` also accepts:

- `max_table_nesting` (default `1000`) — maximum nested/child-table depth.
- `loader_parallelism_strategy` (default `sequential`) — managed-database loads lock at the catalog level, so different tables in the same database can't load concurrently. Override only if you know your loads won't contend for the same database.

## Write modes

Each resource can control how its data lands in the table:

| Mode | What it does |
|------|-------------|
| `replace` | Deletes everything in the table and loads the new batch. Good for full refreshes. |
| `append` | Adds new rows to the table without touching existing data. Good for event logs and immutable records. |
| `merge` (= `upsert`) | Updates existing rows by primary key, inserts new ones. Good for syncing a source of truth. |

> dlt resources set `write_disposition` to `append`, `replace`, or `merge` only. `merge` performs upsert-by-primary-key — it is what the internal `upsert` disposition resolves to, so there is no separate `upsert` to set. The destination also implements an `insert-only` combine (insert rows whose key isn't already present, never updating existing rows), but dlt does not expose it as a resource `write_disposition`, so it cannot be selected per resource.

Set the default for all resources on the destination:

```python
hotdata(write_disposition="replace", ...)
```

Or set it per resource — this takes priority:

```python
@dlt.resource(name="customers", write_disposition="merge", primary_key="id")
def customers_resource():
    ...
```

## Multiple tables

When a pipeline writes to more than one table, pass all table names to `declared_tables`. Hotdata needs to know the full list upfront to set up the managed database correctly.

```python
pipeline = dlt.pipeline(
    pipeline_name="ecommerce",
    destination=hotdata(
        database_name="ecommerce",
        declared_tables=["customers", "orders", "products"],
    ),
)

pipeline.run([customers_resource(), orders_resource(), products_resource()])
```

If you add a new table later, include it in `declared_tables` on the next run — it's added to the existing database in place.

## Verify a load

After a pipeline runs, use the [Hotdata CLI](https://github.com/hotdata-dev/sdk-python) to check that the data landed:

```bash
# List your managed databases
hotdata databases list

# Check that tables are loaded and queryable
hotdata databases tables list --database sales

# Query the data
hotdata query "SELECT * FROM public.orders LIMIT 5" -d sales
```

## Demo pipeline

The package includes a demo that downloads 9 macro-economic indicators from the Federal Reserve (FRED) and loads them into Hotdata. It's a good reference for how a real pipeline is structured.

```bash
export HOTDATA_API_KEY=your_api_key
uv run hotdata-dlt-demo --workspace-id your_workspace_id
```

This creates an `example_macro` database with two tables:

- `macro_indicators_raw` — one row per `(date, series, value)`, all 9 series at their original frequency
- `macro_wide` — one row per month from 1992 onward, each indicator as its own column

## How it works

Each pipeline run:

1. dlt serializes your data to Parquet
2. The Parquet file is uploaded to Hotdata
3. `load_managed_table` applies it with a native load mode

`replace` and `append` apply the uploaded batch directly. `merge` maps to a native `upsert` — the server matches rows by the table's declared key (from your resource's `primary_key`, declared on the table at setup) and updates the matches while inserting the rest — with no full-table read. Only `insert-only`, and a `merge` on a table without a resolvable `primary_key`, still read the current contents, combine in Python (falling back to dlt's `_dlt_id`), and replace. This is all transparent — your resource just yields rows.

The destination preserves dlt's native `_dlt_id` / `_dlt_load_id` columns and persists dlt's schema-version, load, and pipeline-state tables in the managed database so incremental sources can restore their state on the next run. No extra columns are added.

See [docs/architecture.md](docs/architecture.md) and the [runbook](docs/runbook.md) for the internals.

## Development

The project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/hotdata-dev/hotdata-dlt-destination.git
cd hotdata-dlt-destination

uv sync                 # install deps (including dev group)

uv run pytest           # run the test suite
uv run ruff check       # lint
uv run ruff format      # format
uv run mypy             # strict type-check
```

The test suite runs entirely offline — end-to-end tests exercise real dlt pipelines through the destination against an in-memory backend, so no Hotdata credentials are needed.

## Contributing

Issues and pull requests are welcome. Please:

- Open an issue to discuss substantial changes before starting.
- Keep the suite green (`uv run pytest`) and the lint/type checks clean (`uv run ruff check`, `uv run mypy`).
- Add a note to [CHANGELOG.md](CHANGELOG.md) under `[Unreleased]`.

Release process is documented in [RELEASING.md](RELEASING.md).

## License

[MIT](LICENSE) © Hotdata Inc.

## Resources

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [hotdata-framework](https://github.com/hotdata-dev/sdk-python-framework)
- [dlt documentation](https://dlthub.com/docs)
- [Architecture](docs/architecture.md) · [Runbook](docs/runbook.md) · [SQL-client spec](docs/sql-client-spec.md)
- [Changelog](CHANGELOG.md)
