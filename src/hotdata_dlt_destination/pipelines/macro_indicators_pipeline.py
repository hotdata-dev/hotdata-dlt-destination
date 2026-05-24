"""
Macro indicators pipeline — ~/Example edition.

Reads FRED economic indicator CSVs from ~/Example using ibis + DuckDB,
transforms them into clean named columns, joins the monthly series into a
unified wide table, then loads everything into a Hotdata managed database
via the dlt destination.

Resources loaded:
  - cpi                  (CPIAUCSL — Consumer Price Index)
  - fed_funds_rate       (FEDFUNDS — Federal Funds Rate)
  - unemployment_rate    (UNRATE   — Unemployment Rate)
  - housing_starts       (HOUST    — Housing Starts)
  - industrial_production(INDPRO   — Industrial Production Index)
  - mortgage_30yr        (MORTGAGE30US — 30-Year Fixed Mortgage Rate)
  - nonfarm_payroll      (PAYEMS   — Nonfarm Payroll Employment)
  - retail_sales         (RSXFS    — Retail Sales)
  - yield_curve_spread   (T10Y2Y   — 10Y-2Y Treasury Yield Spread)
  - macro_wide           joined table of monthly series (1992 onward)

Requirements:
    pip install dlt ibis-framework[duckdb] hotdata-dlt-destination

Environment:
    HOTDATA_API_KEY    — Hotdata API key
    HOTDATA_WORKSPACE  — Hotdata workspace ID
"""
from __future__ import annotations

import os
from pathlib import Path

import dlt
import ibis
import pandas as pd

from hotdata_dlt_destination.destination import hotdata_destination

EXAMPLE_DIR = Path.home() / "Example"

# Map: resource name -> (csv filename, raw column name)
INDICATORS: dict[str, tuple[str, str]] = {
    "cpi":                   ("cpi.csv",                  "CPIAUCSL"),
    "fed_funds_rate":        ("fed_funds_rate.csv",        "FEDFUNDS"),
    "unemployment_rate":     ("unemployment_rate.csv",     "UNRATE"),
    "housing_starts":        ("housing_starts.csv",        "HOUST"),
    "industrial_production": ("industrial_production.csv", "INDPRO"),
    "mortgage_30yr":         ("mortgage_30yr.csv",         "MORTGAGE30US"),
    "nonfarm_payroll":       ("nonfarm_payroll.csv",       "PAYEMS"),
    "retail_sales":          ("retail_sales.csv",          "RSXFS"),
    "yield_curve_spread":    ("yield_curve_spread.csv",    "T10Y2Y"),
}

# Monthly series available from 1992 onward — used for the wide join
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


def _connect() -> ibis.BaseBackend:
    """Return an in-memory DuckDB connection via ibis."""
    return ibis.duckdb.connect()


def _read_indicator(con: ibis.BaseBackend, name: str) -> ibis.Expr:
    """Read one CSV and rename columns to (date, <name>)."""
    filename, raw_col = INDICATORS[name]
    path = str(EXAMPLE_DIR / filename)
    tbl = con.read_csv(path)
    return tbl.rename(date="observation_date", **{name: raw_col})


@dlt.resource(name="macro_indicators_raw", write_disposition="replace")
def all_indicators_resource():
    """
    Yields one row per (date, series, value) — a long/tidy format table
    covering all nine FRED indicators.
    """
    con = _connect()
    rows: list[dict] = []
    for name, (filename, raw_col) in INDICATORS.items():
        df = _read_indicator(con, name).execute()
        for _, row in df.iterrows():
            if pd.isna(row[name]):
                continue
            rows.append({
                "date": str(row["date"]),
                "series": name,
                "value": float(row[name]),
            })
    yield rows


@dlt.resource(name="macro_wide", write_disposition="replace")
def macro_wide_resource():
    """
    Yields one row per date with each indicator as its own column.
    Inner-joins all monthly series so only fully-populated dates are included
    (retail_sales starts 1992, which sets the floor).
    """
    con = _connect()

    base = _read_indicator(con, MONTHLY_SERIES[0])
    for name in MONTHLY_SERIES[1:]:
        right = _read_indicator(con, name)
        base = base.join(right, "date")

    df = (
        base
        .select(["date"] + MONTHLY_SERIES)
        .order_by("date")
        .execute()
    )

    # Cast date to string so dlt can serialize it cleanly
    df["date"] = df["date"].astype(str)
    yield df.to_dict(orient="records")


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
