# hotdata-dlt-destination

`hotdata-dlt-destination` is a Python package that implements a custom [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/destination) for loading data into **Hotdata managed databases** with deterministic idempotency keys and explicit write semantics.

## What this repo includes

- Custom destination via `@dlt.destination` in `src/hotdata_dlt_destination/destination.py`
- Managed-database ingestion through `hotdata-runtime` (`upload_parquet`, `load_managed_table`, SQL merge)
- Deterministic batch and row idempotency keys
- Example pipelines:
  - `hotdata-dlt-basic-pipeline` (append)
  - `hotdata-dlt-incremental-pipeline` (upsert/merge)
  - `hotdata-dlt-linear-pipeline` (Linear issues → Hotdata)
- Unit tests in `tests/`
- Architecture and runbook docs in `docs/`

## Data contract defaults

- Managed database: `database_name` (default `dlt`, created on first load when missing)
- Schema: `public`
- Table name: normalized lowercase dlt table identifier
- Nested table names: `{parent}__{child}`
- Staging tables: `_dlt_staging_{table}` for append/merge batches
- Write semantics:
  - `append`: parquet upload → staging load → SQL insert into target
  - `replace`: parquet upload → direct managed-table replace on target
  - `upsert`/`merge`: parquet upload → staging load → SQL delete by `_hotdata_row_key` + insert
- Idempotency:
  - Batch key `_hotdata_batch_key` = hash(table + full batch payload)
  - Row key `_hotdata_row_key` = hash(table + canonical row payload)

## Configure

Set environment variables (or pass destination kwargs / dlt secrets):

- `HOTDATA_API_KEY`
- `HOTDATA_WORKSPACE`
- `HOTDATA_DATABASE` (managed database name, default `dlt`)
- optional: `HOTDATA_SCHEMA`, `HOTDATA_WRITE_DISPOSITION`, retry tuning

## Usage

```python
import dlt
from hotdata_dlt_destination import hotdata_destination

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=hotdata_destination(
        database_name="analytics",
        write_disposition="append",
    ),
)
pipeline.run(my_resource())
```

## Developer workflow

```bash
uv sync
uv run ruff check .
uv run pytest
uv run hotdata-dlt-destination
```

Run pipelines:

```bash
uv run hotdata-dlt-basic-pipeline
uv run hotdata-dlt-incremental-pipeline
uv run hotdata-dlt-linear-pipeline
```

Run the live end-to-end integration test (requires Hotdata + Linear env vars):

```bash
uv run pytest tests/test_e2e_linear_hotdata.py -m integration
```

## References

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python)
- [hotdata-runtime](https://github.com/hotdata-dev/hotdata-runtime)
- [dlt custom destination](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
