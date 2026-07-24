"""
Macro indicators demo pipeline.

Downloads FRED economic indicator data directly from fred.stlouisfed.org
and loads it into a Hotdata managed database.

Resources loaded:
  - macro_indicators_raw  (date, series, value -- long/tidy format, raw frequency)
  - macro_wide            one row per month, all indicators as columns (1992 onward)

Environment:
    HOTDATA_API_KEY  -- Hotdata API key

Usage:
    hotdata-dlt-demo --workspace-id <id>
"""

from __future__ import annotations

import argparse
import functools
import io
import os
import urllib.request

import dlt
import pandas as pd

from hotdata_dlt_destination import hotdata
from hotdata_dlt_destination.configuration import HotdataCredentials

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"

INDICATORS: dict[str, str] = {
    "cpi": "CPIAUCSL",
    "fed_funds_rate": "FEDFUNDS",
    "unemployment_rate": "UNRATE",
    "housing_starts": "HOUST",
    "industrial_production": "INDPRO",
    "mortgage_30yr": "MORTGAGE30US",
    "nonfarm_payroll": "PAYEMS",
    "retail_sales": "RSXFS",
    "yield_curve_spread": "T10Y2Y",
}


@functools.cache
def _download_indicator(name: str) -> pd.DataFrame:
    """Download a FRED series and return a DataFrame with columns (date, <name>)."""
    series_id = INDICATORS[name]
    with urllib.request.urlopen(FRED_URL.format(series_id), timeout=30) as response:
        content = response.read().decode("utf-8")
    df = pd.read_csv(io.StringIO(content), parse_dates=["observation_date"], na_values=["."])
    return df.rename(columns={"observation_date": "date", series_id: name})


def _to_monthly(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Resample a series to month-start frequency, taking the last value per month."""
    return df.set_index("date")[name].resample("MS").last().dropna().reset_index()


@dlt.resource(name="macro_indicators_raw", write_disposition="replace")
def all_indicators_resource():
    """Yields one row per (date, series, value) -- long/tidy format at raw frequency."""
    rows: list[dict] = []
    for name in INDICATORS:
        df = _download_indicator(name).copy().dropna(subset=[name])
        df["date"] = df["date"].astype(str)
        rows.extend(df.rename(columns={name: "value"}).assign(series=name).to_dict("records"))
    yield rows


@dlt.resource(name="macro_wide", write_disposition="replace")
def macro_wide_resource():
    """Yields one row per month with each indicator as its own column (inner-joined, 1992 onward).

    All series are resampled to month-start before joining so that weekly
    (MORTGAGE30US) and daily (T10Y2Y) series align with monthly ones.
    """
    dfs = [_to_monthly(_download_indicator(name), name) for name in INDICATORS]
    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.merge(df, on="date", how="inner")

    merged = merged.sort_values("date")
    merged["date"] = merged["date"].astype(str)
    yield merged[["date", *INDICATORS]].to_dict(orient="records")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load FRED macro indicators into Hotdata.")
    parser.add_argument("--workspace-id", required=True, help="Hotdata workspace id")
    parser.add_argument(
        "--database-id",
        default=None,
        help="Existing managed database id to load into (printed on first-run create; "
        "omit to create a new database by name)",
    )
    args = parser.parse_args()
    all_tables = ["macro_indicators_raw", "macro_wide"]

    pipeline = dlt.pipeline(
        pipeline_name="macro_indicators",
        destination=hotdata(
            credentials=HotdataCredentials(api_key=os.environ["HOTDATA_API_KEY"]),
            workspace_id=args.workspace_id,
            database_id=args.database_id,
            api_base_url=os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev"),
            write_disposition="replace",
            declared_tables=all_tables,
            database_name="example_macro",
            create_database_if_missing=True,
        ),
        dataset_name="example_macro",
    )

    load_info = pipeline.run([all_indicators_resource(), macro_wide_resource()])
    print(load_info)


if __name__ == "__main__":
    main()
