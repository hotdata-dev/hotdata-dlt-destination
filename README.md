# hotdata-dlt-destination

`hotdata-dlt-destination` is a Python package that implements a custom [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/destination) for loading data into **Hotdata managed databases** with deterministic idempotency keys and explicit write semantics.

## What this repo includes

- Custom destination via `@dlt.destination` in `src/hotdata_dlt_destination/destination.py`
- Managed-database ingestion through `hotdata-runtime` (`upload_parquet`, `load_managed_table`, `SELECT`)
- Read-modify-write append/merge using only supported API operations
- Deterministic batch and row idempotency keys
- Demo pipeline (`hotdata-dlt-demo`): downloads 9 FRED macro indicators and loads them into Hotdata
- Unit tests in `tests/`
- Architecture and runbook docs in `docs/`

## Data contract defaults

- Managed database: `database_name` (default `dlt`, created on first load when missing)
- Schema: `public`
- Table name: normalized lowercase dlt table identifier
- Nested table names: `{parent}__{child}`
- Write semantics (all use `load_managed_table(replace)` under the hood):
  - `replace`: upload batch parquet and replace the target table
  - `append`: read existing target rows, append batch in Python, replace target
  - `upsert`/`merge`: read existing rows, upsert by dlt `primary_key` (or `_hotdata_row_key`), replace target
- Idempotency:
  - Batch key `_hotdata_batch_key` = hash(table + full batch payload)
  - Row key `_hotdata_row_key` = hash(table + canonical row payload)

## Configure

Set environment variables (or pass destination kwargs / dlt secrets):

- `HOTDATA_API_KEY`
- `HOTDATA_WORKSPACE`
- `HOTDATA_DATABASE` (managed database name, default `dlt`)
- optional: `HOTDATA_SCHEMA`, `HOTDATA_WRITE_DISPOSITION`, `HOTDATA_DECLARED_TABLES`, retry tuning

For pipelines with multiple tables, declare every target table when the managed database is first created:

```python
hotdata_destination(
    database_name="analytics",
    declared_tables=["customers", "orders", "orders__items"],
)
```

## Usage

```python
import dlt
from hotdata_dlt_destination import hotdata_destination

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=hotdata_destination(
        database_name="analytics",
        write_disposition="append",
        declared_tables=["customers"],
    ),
)
pipeline.run(my_resource())
```

Per-resource `write_disposition` and `primary_key` from dlt take precedence over the destination default.

## Demo

The demo pipeline downloads 9 FRED economic indicator series from `fred.stlouisfed.org` and loads two tables into a Hotdata managed database named `example_macro`:

| Table | Description |
|-------|-------------|
| `macro_indicators_raw` | Long/tidy format — one row per `(date, series, value)` |
| `macro_wide` | Wide format — one row per date, each indicator as its own column (inner-joined, 1992 onward) |

**Run it:**

```bash
export HOTDATA_API_KEY=...
export HOTDATA_WORKSPACE=...
uv run hotdata-dlt-demo
```

```
Pipeline macro_indicators load step completed in 1.44 seconds
1 load package(s) were loaded to destination hotdata and into dataset None
Load package 1779654466.552855 is LOADED and contains no failed jobs
```

**Verify with the Hotdata CLI:**

```bash
# Confirm the database was created
hotdata databases list

# Check both tables are synced
hotdata databases tables list --database example_macro

# Count rows per series in the long-format table
hotdata query "SELECT series, COUNT(*) AS cnt FROM default.public.macro_indicators_raw GROUP BY series ORDER BY series" -d example_macro

# Preview the wide table
hotdata query "SELECT * FROM default.public.macro_wide ORDER BY date LIMIT 5" -d example_macro
```

Expected output for the row count query:

```
industrial_production  1288
cpi                     951
unemployment_rate       939
fed_funds_rate          862
housing_starts          808
nonfarm_payroll        1048
retail_sales            412
mortgage_30yr          2878
yield_curve_spread    12491
```

## Developer workflow

```bash
uv sync
uv run ruff check .
uv run pytest
uv run hotdata-dlt-destination  # validate config
uv run hotdata-dlt-demo         # run the demo pipeline
```

## References

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [hotdata-runtime](https://github.com/hotdata-dev/hotdata-runtime)
- [dlt custom destination](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
