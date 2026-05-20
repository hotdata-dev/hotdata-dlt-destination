from __future__ import annotations

import os
from datetime import UTC, datetime

import dlt
import pytest

from hotdata_dlt_destination.destination import hotdata_destination
from hotdata_dlt_destination.pipelines.linear_pipeline import fetch_linear_issues


def _missing_env_vars() -> list[str]:
    required = ["HOTDATA_API_KEY", "HOTDATA_WORKSPACE", "LINEAR_API_KEY"]
    return [key for key in required if not os.environ.get(key)]


@pytest.mark.integration
def test_e2e_linear_to_hotdata() -> None:
    missing = _missing_env_vars()
    if missing:
        pytest.skip(f"Missing environment variables for e2e test: {', '.join(missing)}")

    run_suffix = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    linear_rows = fetch_linear_issues(
        api_key=os.environ["LINEAR_API_KEY"],
        team_key=os.environ.get("LINEAR_TEAM_KEY"),
        issue_limit=int(os.environ.get("LINEAR_ISSUE_LIMIT", "10")),
    )
    assert linear_rows

    pipeline = dlt.pipeline(
        pipeline_name=f"hotdata_linear_e2e_{run_suffix}",
        destination=hotdata_destination(
            api_key=os.environ["HOTDATA_API_KEY"],
            workspace_id=os.environ["HOTDATA_WORKSPACE"],
            api_base_url=os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev"),
            write_disposition="upsert",
            database_name=f"linear_e2e_{run_suffix}",
        ),  # type: ignore[call-arg]
        dataset_name=f"hotdata_linear_e2e_{run_suffix}",
    )
    load_info = pipeline.run(linear_rows, table_name="linear_issues")

    assert load_info.loads_ids
