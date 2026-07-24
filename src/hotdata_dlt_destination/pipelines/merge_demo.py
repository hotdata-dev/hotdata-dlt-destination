"""
Composite-key merge + hard_delete demo pipeline.

Loads a small ``orders`` table into a Hotdata managed database in two runs to
show off native merge/upsert by a composite key and dlt's ``hard_delete`` hint:

  - load 1 (replace): seeds a brand-new table with no declared key.
  - load 2 (merge, primary_key=["region", "order_id"]): updates three orders,
    inserts one, and hard-deletes one -- matched by the per-load composite key,
    even though the table was never created with a key.

The merge key rides each load (resolved from the resource's ``primary_key``), so
no key is declared when the table is created. Rows flagged in the ``deleted``
column (dlt's ``hard_delete`` hint) are removed by key; every other row upserts.

Environment:
    HOTDATA_API_KEY  -- Hotdata API key

Usage:
    hotdata-dlt-merge-demo --workspace-id <id>
"""

from __future__ import annotations

import argparse
import os

import dlt

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import HotdataCredentials

DATABASE = "example_orders"
SCHEMA = "public"


@dlt.resource(name="orders", write_disposition="replace")
def orders_seed():
    """Seed 5 orders across 3 regions into a brand-new (keyless) table.

    Uses ``replace`` so the demo is idempotent: re-running resets the seed
    instead of appending duplicate rows. ``replace`` declares no key, so the
    table is still created keyless -- the merge load below matches purely on the
    per-load key. ``deleted`` is included here only so the hard_delete column
    exists on the table before the merge load references it.
    """
    yield [
        {"region": "us", "order_id": 1, "customer": "Alice", "amount": 100, "status": "pending", "deleted": False},
        {"region": "us", "order_id": 2, "customer": "Bob", "amount": 50, "status": "pending", "deleted": False},
        {"region": "eu", "order_id": 1, "customer": "Carla", "amount": 200, "status": "pending", "deleted": False},
        {"region": "eu", "order_id": 2, "customer": "Dan", "amount": 75, "status": "shipped", "deleted": False},
        {"region": "apac", "order_id": 1, "customer": "Emi", "amount": 300, "status": "pending", "deleted": False},
    ]


@dlt.resource(
    name="orders",
    write_disposition="merge",
    primary_key=["region", "order_id"],
    columns={"deleted": {"hard_delete": True}},
)
def orders_changes():
    """Merge by the composite key: update 3 orders, insert 1, hard-delete 1.

    Kept rows omit ``deleted`` (a boolean hard_delete column deletes only on
    ``True``); only the deleted row sets it.
    """
    yield [
        {"region": "us", "order_id": 1, "customer": "Alice", "amount": 100, "status": "shipped"},
        {"region": "eu", "order_id": 1, "customer": "Carla", "amount": 220, "status": "paid"},
        {"region": "eu", "order_id": 2, "customer": "Dan", "amount": 75, "status": "delivered"},
        {"region": "us", "order_id": 3, "customer": "Frank", "amount": 40, "status": "pending"},
        {"region": "apac", "order_id": 1, "customer": "Emi", "amount": 300, "status": "pending", "deleted": True},
    ]


def _print_table(pipeline: dlt.Pipeline, label: str) -> None:
    df = pipeline.dataset().table("orders").df()
    df = df[["region", "order_id", "customer", "amount", "status"]].sort_values(["region", "order_id"])
    print(f"\n== {label} ({len(df)} rows) ==")
    print(df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Composite-key merge + hard_delete demo.")
    parser.add_argument("--workspace-id", required=True, help="Hotdata workspace id")
    parser.add_argument(
        "--database-id",
        default=None,
        help="Existing managed database id (omit to create a new one by name)",
    )
    args = parser.parse_args()
    api_base_url = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")

    # id-first: address the managed database by id (names are not identifiers), so
    # the before/after reads below can find it. Provision it once and pin the id;
    # pass --database-id to reuse an existing database.
    database_id = args.database_id
    if database_id is None:
        from hotdata_framework.client import HotdataClient as RuntimeClient

        rc = RuntimeClient(
            os.environ["HOTDATA_API_KEY"], args.workspace_id, host=api_base_url.rstrip("/")
        )
        database_id = rc.create_managed_database(
            description=DATABASE, schema=SCHEMA, tables=["orders"]
        ).id
        rc.close()
        print(f"created managed database {database_id} (pass --database-id {database_id} to reuse)")

    pipeline = dlt.pipeline(
        pipeline_name="orders_merge",
        destination=hotdata(
            credentials=HotdataCredentials(api_key=os.environ["HOTDATA_API_KEY"]),
            workspace_id=args.workspace_id,
            database_id=database_id,
            api_base_url=api_base_url,
            declared_tables=["orders"],
            database_name=DATABASE,
            schema=SCHEMA,
            create_database_if_missing=True,
        ),
        dataset_name=SCHEMA,
    )

    print(pipeline.run(orders_seed()))
    _print_table(pipeline, "after load 1 (replace, keyless)")

    print(pipeline.run(orders_changes()))
    _print_table(pipeline, "after load 2 (merge by [region, order_id] + hard_delete)")


if __name__ == "__main__":
    main()
