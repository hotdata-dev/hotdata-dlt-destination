from __future__ import annotations

from datetime import UTC, datetime

import dlt

from hotdata_dlt_destination.destination import hotdata_destination


@dlt.resource(name="orders", write_disposition="merge", primary_key="id")
def orders_resource(since: str = "2026-01-01T00:00:00+00:00") -> list[dict[str, object]]:
    watermark = datetime.fromisoformat(since)
    rows = [
        {"id": 101, "status": "new", "updated_at": "2026-01-01T01:00:00+00:00", "amount": 120.50},
        {"id": 102, "status": "paid", "updated_at": "2026-01-02T01:00:00+00:00", "amount": 50.00},
        {"id": 101, "status": "paid", "updated_at": "2026-01-03T01:00:00+00:00", "amount": 120.50},
    ]
    return [
        row
        for row in rows
        if datetime.fromisoformat(str(row["updated_at"])).replace(tzinfo=UTC) > watermark
    ]


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hotdata_incremental",
        destination=hotdata_destination(write_disposition="upsert"),  # type: ignore[call-arg]
        dataset_name="hotdata_incremental",
    )
    load_info = pipeline.run(orders_resource())
    print(load_info)


if __name__ == "__main__":
    main()
