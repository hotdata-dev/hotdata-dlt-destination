# Runbook

## Local development

1. Install dependencies:

   ```bash
   uv sync
   ```

2. Configure environment:

   ```bash
   cp .env.example .env
   ```

3. Export environment values (or use your shell env loader).

4. Validate tooling:

   ```bash
   uv run ruff check .
   uv run pytest
   ```

## Run pipelines

- Demo pipeline (downloads FRED macro-economic data and loads into Hotdata):

  ```bash
  uv run hotdata-dlt-demo
  ```

  Requires `HOTDATA_API_KEY` and `HOTDATA_WORKSPACE` to be set.

## Add a new pipeline

1. Create a new module in `src/hotdata_dlt_destination/pipelines/`.
2. Define a `dlt.resource` with explicit `name` and `write_disposition`.
3. Build pipeline with `destination=hotdata(database_name=..., declared_tables=[...])`.
4. Add script entrypoint in `pyproject.toml`.
5. Add tests covering schema shape, write-disposition behavior, and retry/error handling.

## Failure recovery

- Retryable failures auto-retry based on `HOTDATA_MAX_RETRIES` and `HOTDATA_RETRY_BACKOFF_SECONDS`.
- If a load fails after max retries, rerun the same pipeline. Deterministic row keys keep upsert behavior stable.
- For append mode recovery, rerun at the same source checkpoint to replay failed batches.

## Troubleshooting

- `401` / `403`: verify `HOTDATA_API_KEY` and `HOTDATA_WORKSPACE`.
- `404` on destination paths: verify `HOTDATA_API_BASE_URL` is the API host (for example `https://api.hotdata.dev`).
- `429` / `5xx`: increase retry/backoff values.
- `table not declared`: recreate the managed database with all target tables in `declared_tables`, or declare them at create time.
- Append/merge loads re-read the full target table each batch; large tables may be slow until native append/merge API support lands.
