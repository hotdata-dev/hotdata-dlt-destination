# hotdata-dlt-destination

Load data into [Hotdata](https://hotdata.dev) managed databases using [dlt](https://dlthub.com).

dlt handles extraction, schema inference, and batching. This package handles the Hotdata side — uploading each batch as Parquet and registering it with your managed database.

## Install

```bash
pip install hotdata-dlt-destination
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
        database_name="sales",
        declared_tables=["orders"],
    ),
)

pipeline.run(orders_resource())
```

Set your credentials as environment variables before running:

```bash
export HOTDATA_API_KEY=your_api_key
export HOTDATA_WORKSPACE=your_workspace_id
```

That's it. On first run, the `sales` managed database is created automatically and the `orders` table is loaded.

`hotdata` is a native dlt destination (`JobClientBase` + `WithStateSync`): it supports nested/child tables, preserves dlt's internal columns (`_dlt_id`, `_dlt_load_id`), and persists schema-version, load, and pipeline-state tables in the managed database so **incremental sources resume correctly across runs**. If an existing managed database is missing a declared table on a later run, the table is added to it in place; existing tables and their data are left untouched.

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

You can also author queries with **ibis** — `dataset().table("orders").to_ibis()` gives an ibis table; build an expression and dlt compiles it to SQL and runs it through the same client. (The live ibis *backend*, `dataset().ibis()`, is not supported — dlt wires that to a direct engine connection per destination, which Hotdata's remote engine doesn't expose.)

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
| Live ibis backend (`dataset().ibis()`) | ❌ | dlt maps this to a direct per-destination engine connection; Hotdata's engine is remote (REST + DuckLake), not a wire-protocol DB or local files |
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
| `api_key` | `HOTDATA_API_KEY` | required | Your Hotdata API key |
| `workspace_id` | `HOTDATA_WORKSPACE` | required | Your Hotdata workspace ID |
| `database_name` | `HOTDATA_DATABASE` | `dlt` | Managed database to load into |
| `schema` | `HOTDATA_SCHEMA` | `public` | Schema within the managed database |
| `write_disposition` | `HOTDATA_WRITE_DISPOSITION` | `append` | Default write mode (see below) |
| `declared_tables` | `HOTDATA_DECLARED_TABLES` | — | All table names the pipeline will write (required for multi-table pipelines — see below) |
| `create_database_if_missing` | — | `True` | Create the managed database if it doesn't exist yet |
| `max_retries` | `HOTDATA_MAX_RETRIES` | `5` | How many times to retry a failed request |
| `retry_backoff_seconds` | `HOTDATA_RETRY_BACKOFF_SECONDS` | `1.0` | Initial wait between retries (grows with each attempt) |

You can pass any of these as keyword arguments to `hotdata(...)`, or set the corresponding environment variable. `hotdata` also accepts `max_table_nesting` (default `1000`).

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

If you add a new table later, include it in `declared_tables` on the next run.

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
export HOTDATA_WORKSPACE=your_workspace_id
uv run hotdata-dlt-demo
```

This creates a `example_macro` database with two tables:

- `macro_indicators_raw` — one row per `(date, series, value)`, all 9 series at their original frequency
- `macro_wide` — one row per month from 1992 onward, each indicator as its own column

## How it works

Each pipeline run:

1. dlt serializes your data to Parquet
2. The Parquet file is uploaded to Hotdata
3. `load_managed_table` applies it with a native load mode

`replace` and `append` apply the uploaded batch directly. `merge` maps to a native `upsert` — the server matches rows by the table's declared key (from your resource's `primary_key`, declared on the table at setup) and updates the matches while inserting the rest — with no full-table read. Only `insert-only`, and a `merge` on a table without a resolvable `primary_key`, still read the current contents, combine in Python (falling back to dlt's `_dlt_id`), and replace. This is all transparent — your resource just yields rows.

The destination preserves dlt's native `_dlt_id` / `_dlt_load_id` columns and persists dlt's schema-version, load, and pipeline-state tables in the managed database so incremental sources can restore their state on the next run. No extra columns are added.

## Resources

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [hotdata-framework](https://github.com/hotdata-dev/sdk-python-framework)
- [dlt documentation](https://dlthub.com/docs)
- [Architecture and runbook](docs/runbook.md)
