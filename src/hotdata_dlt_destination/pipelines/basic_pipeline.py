from __future__ import annotations

import dlt

from hotdata_dlt_destination.destination import hotdata_destination


@dlt.resource(name="customers", write_disposition="append")
def customers_resource() -> list[dict[str, object]]:
    return [
        {"id": 1, "name": "Acme", "tier": "enterprise", "is_active": True},
        {"id": 2, "name": "Globex", "tier": "startup", "is_active": True},
        {"id": 3, "name": "Initech", "tier": "midmarket", "is_active": False},
    ]


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hotdata_basic",
        destination=hotdata_destination(
            write_disposition="append",
            declared_tables=["customers"],
        ),  # type: ignore[call-arg]
        dataset_name="hotdata_basic",
    )
    load_info = pipeline.run(customers_resource())
    print(load_info)


if __name__ == "__main__":
    main()
