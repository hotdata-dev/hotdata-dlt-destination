"""dlt <-> Hotdata round trip: write a table with dlt, then read it back.

The write half has always worked; the read half is what this project adds. To
make the difference concrete, the data is read back **two ways**:

  read_with_dlt      -- the new way: dlt's own dataset API (what we built)
  read_without_dlt   -- the old way: drop out of dlt, query Hotdata by hand

Run against hosted Hotdata (the default) or any Hotdata API. `.env` is not
auto-loaded, so source it first and pass your workspace id as an argument:

    set -a; source .env; set +a
    uv run python scripts/roundtrip_demo.py --workspace-id <id>

Point at a local cluster instead by exporting HOTDATA_API_BASE_URL=http://api.localhost
(plus a local key) before running.

Env:
    HOTDATA_API_KEY       -- required
    HOTDATA_API_BASE_URL  -- optional (default https://api.hotdata.dev)
"""

from __future__ import annotations

import argparse
import os

import dlt

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import HotdataCredentials

DATABASE = "roundtrip_demo"
API_BASE_URL = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")


@dlt.resource(name="spans", write_disposition="merge", primary_key="span_id")
def spans():
    """A tiny, self-contained table of LLM-request telemetry."""
    yield [
        {"span_id": "a1", "model": "claude-opus-4-8", "latency_ms": 812, "ok": True},
        {"span_id": "a2", "model": "claude-sonnet-5", "latency_ms": 240, "ok": True},
        {"span_id": "a3", "model": "claude-opus-4-8", "latency_ms": 590, "ok": False},
    ]


def read_with_dlt(pipeline: dlt.Pipeline) -> None:
    """The new way -- read through dlt's own dataset API.

    No Hotdata imports, no credentials, no database ids, no table qualification:
    the *same* ``pipeline`` object that wrote the data reads it back. This is
    byte-for-byte how you'd read any dlt destination (duckdb, postgres, bigquery).
    """
    ds = pipeline.dataset()

    print("-- full table --")
    print(ds.table("spans").df())

    print("\n-- aggregate (raw SQL) --")
    print(
        ds(
            "SELECT model, avg(latency_ms) AS avg_latency_ms "
            "FROM spans GROUP BY model ORDER BY model"
        ).df()
    )

    print("\n-- fluent: ok rows, fastest first, top 2 --")
    print(ds.table("spans").select("span_id", "model", "latency_ms").where("ok").order_by(
        "latency_ms"
    ).limit(2).df())

    print("\n-- arrow --")
    print(ds.table("spans").arrow().schema)


def read_without_dlt(workspace_id: str) -> None:
    """The old way -- drop out of dlt and query Hotdata directly.

    Illustrative only (NOT shipped, NOT how we want users to read). It shows the
    friction ``dataset()`` removes: re-instantiate a Hotdata client with
    credentials, address the instant database by name, fully-qualify the table as
    ``"default"."public"."spans"`` (the catalog is always ``default``), hand-write
    the SQL, and marshal the QueryResult into a DataFrame yourself.
    """
    from hotdata_framework.client import HotdataClient as RuntimeClient

    rt = RuntimeClient(
        os.environ["HOTDATA_API_KEY"],
        workspace_id,
        host=API_BASE_URL,
    )
    try:
        result = rt.execute_sql(
            'SELECT model, avg(latency_ms) AS avg_latency_ms '
            'FROM "default"."public"."spans" GROUP BY model ORDER BY model',
            database=DATABASE,
        )
        print(result.to_pandas())
    finally:
        rt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="dlt <-> Hotdata round trip demo.")
    parser.add_argument("--workspace-id", required=True, help="Hotdata workspace id")
    parser.add_argument(
        "--database-id",
        default=None,
        help="Existing instant database id (omit to create a new one by name)",
    )
    args = parser.parse_args()
    workspace_id = args.workspace_id

    # id-first: an instant database is addressed by id (names are not identifiers),
    # so a read-back must know the id. Provision the database once here and pin its
    # id for both the write and the read; pass --database-id to reuse an existing one.
    database_id = args.database_id
    if database_id is None:
        from hotdata_framework.client import HotdataClient as RuntimeClient

        rc = RuntimeClient(os.environ["HOTDATA_API_KEY"], workspace_id, host=API_BASE_URL.rstrip("/"))
        database_id = rc.create_managed_database(
            description=DATABASE, schema="public", tables=["spans"]
        ).id
        rc.close()
        print(f"created instant database {database_id} (pass --database-id {database_id} to reuse)")

    pipeline = dlt.pipeline(
        pipeline_name="roundtrip_demo",
        destination=hotdata(
            credentials=HotdataCredentials(api_key=os.environ["HOTDATA_API_KEY"]),
            workspace_id=workspace_id,
            database_id=database_id,
            database_name=DATABASE,
            declared_tables=["spans"],
            create_database_if_missing=True,
            api_base_url=API_BASE_URL,
        ),
        dataset_name="public",
    )

    print("== WRITE (worked before this project) ==")
    print(pipeline.run(spans()))

    print("\n== READ, the new way -- via dlt <> hotdata ==")
    read_with_dlt(pipeline)

    print("\n== READ, the old way -- manual Hotdata SDK (illustrative) ==")
    read_without_dlt(workspace_id)


if __name__ == "__main__":
    main()
