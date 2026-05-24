"""
Macro indicators demo pipeline.

Downloads FRED economic indicator data directly from fred.stlouisfed.org
and loads it into a Hotdata managed database.

Resources loaded:
  - macro_indicators_raw  (date, series, value -- long/tidy format)
  - macro_wide            joined monthly series (1992 onward)

Environment:
    HOTDATA_API_KEY    -- Hotdata API key
    HOTDATA_WORKSPACE  -- Hotdata workspace ID
"""
from __future__ import annotations

import os

import dlt
import pandas as pd

from hotdata_dlt_destination.destination import hotdata_destination

FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"

INDICATORS: dict[str, str] = {
    "cpi":                   "CPIAUCSL",
    "fed_funds_rate":        "FEDFUNDS",
    "unemployment_rate":     "UNRATE",
    "housing_starts":        "HOUST",
    "industrial_production": "INDPRO",
    "mortgage_30yr":         "MORTGAGE30US",
    "nonfarm_payroll":       "PAYEMS",
    "retail_sales":          "RSXFS",
    "yield_curve_spread":    "T10Y2Y",
}

MONTHLY_SERIES = [
    "cpi",
    "fed_funds_rate",
    "unemployment_rate",
    "housing_starts",
    "industrial_production",
    "mortgage_30yr",
    "nonfarm_payroll",
    "retail_sales",
    "yield_curve_spread",
]


def _download_indicator(name: str) -> pd.DataFrame:
    """Download a FRED series and return a DataFrame with columns (date, <name>)."""
    series_id = INDICATORS[name]
    df = pd.read_csv(FRED_URL.format(series_id), parse_dates=["observation_date"])
    return df.rename(columns={"observation_date": "date", series_id: name})


@dlt.resource(name="macro_indicators_raw", write_disposition="replace")
def all_indicators_resource():
    """Yields one row per (date, series, value) -- long/tidy format."""
    rows: list[dict] = []
    for name in INDICATORS:
        df = _download_indicator(name)
        for _, row in df.iterrows():
            if pd.isna(row[name]):
                continue
            rows.append({"date": str(row["date"]), "series": name, "value": float(row[name])})
    yield rows


@dlt.resource(name="macro_wide", write_disposition="replace")
def macro_wide_resource():
    """Yields one row per date with each indicator as its own column (inner-joined)."""
    dfs = [_download_indicator(name) for name in MONTHLY_SERIES]
    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.merge(df, on="date", how="inner")

    merged = merged.sort_values("date")
    merged["date"] = merged["date"].astype(str)
    yield merged[["date"] + MONTHLY_SERIES].to_dict(orient="records")


def main() -> None:
    all_tables = ["macro_indicators_raw", "macro_wide"]

    pipeline = dlt.pipeline(
        pipeline_name="macro_indicators",
        destination=hotdata_destination(
            api_key=os.environ["HOTDATA_API_KEY"],
            workspace_id=os.environ["HOTDATA_WORKSPACE"],
            write_disposition="replace",
            declared_tables=all_tables,
            database_name="example_macro",
            create_database_if_missing=True,
        ),  # type: ignore[call-arg]
        dataset_name="example_macro",
    )

    load_info = pipeline.run([all_indicators_resource(), macro_wide_resource()])
    print(load_info)


if __name__ == "__main__":
    main()
