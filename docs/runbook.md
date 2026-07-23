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
  uv run hotdata-dlt-demo <workspace_id>
  ```

  Requires `HOTDATA_API_KEY` to be set; pass your workspace id as the argument.

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

- `401` / `403`: verify `HOTDATA_API_KEY` and the workspace id you passed.
- `404` on destination paths: verify `HOTDATA_API_BASE_URL` is the API host (for example `https://api.hotdata.dev`).
- `429` / `5xx`: increase retry/backoff values.
- `table not declared`: list every target table in `declared_tables`. Missing tables are otherwise added to the database in place on the next run.
- Append/merge loads re-read the full target table each batch; large tables may be slow until native append/merge API support lands.
