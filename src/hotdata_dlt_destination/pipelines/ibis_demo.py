"""Live ibis backend demo for the Hotdata dlt destination.

Loads a small self-contained dataset into a Hotdata managed database, then reads
it back with ``pipeline.dataset().ibis()`` -- the live ``ibis.hotdata`` backend --
and runs ibis expressions and raw SQL against the remote engine.

Point ``HOTDATA_API_BASE_URL`` at a local cluster or the hosted API; the flow is
otherwise identical.

Environment:
    HOTDATA_API_KEY       -- Hotdata API key
    HOTDATA_API_BASE_URL  -- Hotdata API base URL (default https://api.hotdata.dev)

Usage:
    hotdata-dlt-ibis-demo --workspace-id <id>

Requires the ``[ibis]`` extra (``uv sync --extra ibis``).
"""

from __future__ import annotations

import argparse
import os
import time

import dlt
import ibis

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import HotdataCredentials

DATABASE_NAME = "ibis_demo"
SCHEMA = "public"

TRIPS = [
    {"trip_id": 1, "city": "SF", "rider": "alice", "distance_km": 4.2, "fare": 12.5},
    {"trip_id": 2, "city": "SF", "rider": "bob", "distance_km": 1.1, "fare": 6.0},
    {"trip_id": 3, "city": "NYC", "rider": "alice", "distance_km": 8.9, "fare": 24.0},
    {"trip_id": 4, "city": "NYC", "rider": "carol", "distance_km": 3.3, "fare": 11.0},
    {"trip_id": 5, "city": "SF", "rider": "carol", "distance_km": 6.7, "fare": 18.5},
]


@dlt.resource(name="trips", write_disposition="replace")
def trips_resource():
    """Yields the demo trip rows."""
    yield TRIPS


def _load(pipeline: dlt.Pipeline) -> None:
    """Run the write pipeline and print the load summary."""
    load_info = pipeline.run(trips_resource())
    print(load_info)


def _read_with_ibis(pipeline: dlt.Pipeline) -> None:
    """Read the loaded table back through the live ibis.hotdata backend."""
    con = pipeline.dataset().ibis()
    print(f"connected: backend={con.name!r}")

    trips = con.table("trips", database=("default", SCHEMA))

    by_city = (
        trips.group_by("city")
        .aggregate(
            n=trips.count(),
            avg_fare=trips.fare.mean(),
            total_km=trips.distance_km.sum(),
        )
        .order_by(ibis.desc("total_km"))
    )
    print("\nfares by city (ibis expression):")
    print(by_city.execute())

    top = con.sql(
        "SELECT rider, SUM(fare) AS spend "
        f'FROM "default"."{SCHEMA}"."trips" '
        "GROUP BY rider ORDER BY spend DESC LIMIT 3"
    )
    print("\ntop riders by spend (raw SQL through ibis):")
    print(top.execute())


def main() -> None:
    parser = argparse.ArgumentParser(description="Load NYC taxi trips and read via ibis.")
    parser.add_argument("--workspace-id", required=True, help="Hotdata workspace id")
    parser.add_argument(
        "--database-id",
        default=None,
        help="Existing managed database id (omit to create a new one by name)",
    )
    args = parser.parse_args()
    api_base_url = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")

    # id-first: the live ibis read binds the managed database by id (names are not
    # identifiers). Provision it once and pin the id; pass --database-id to reuse.
    database_id = args.database_id
    if database_id is None:
        from hotdata_framework.client import HotdataClient as RuntimeClient

        rc = RuntimeClient(
            os.environ["HOTDATA_API_KEY"], args.workspace_id, host=api_base_url.rstrip("/")
        )
        database_id = rc.create_managed_database(
            description=DATABASE_NAME, schema=SCHEMA, tables=["trips"]
        ).id
        rc.close()
        print(f"created managed database {database_id} (pass --database-id {database_id} to reuse)")

    pipeline = dlt.pipeline(
        pipeline_name="ibis_demo",
        destination=hotdata(
            credentials=HotdataCredentials(api_key=os.environ["HOTDATA_API_KEY"]),
            workspace_id=args.workspace_id,
            database_id=database_id,
            api_base_url=api_base_url,
            write_disposition="replace",
            declared_tables=["trips"],
            database_name=DATABASE_NAME,
            schema=SCHEMA,
            create_database_if_missing=True,
        ),
        dataset_name=SCHEMA,
    )

    _load(pipeline)
    # Managed-table loads settle asynchronously server-side; give the read a moment.
    time.sleep(2)
    _read_with_ibis(pipeline)


if __name__ == "__main__":
    main()
