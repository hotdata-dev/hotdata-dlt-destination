from __future__ import annotations

from hotdata_dlt_destination.config import HotdataDestinationConfig


def main() -> None:
    config = HotdataDestinationConfig.from_env()
    print("hotdata-dlt-destination is configured")
    print(f"api_base_url={config.api_base_url}")
    print(f"database_id={config.database_id or '<unset — will create by name>'}")
    print(f"database_name={config.database_name}")
    print(f"schema={config.schema}")
    print(f"write_disposition={config.write_disposition}")
