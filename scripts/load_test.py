#!/usr/bin/env python3
"""Load test for hotdata-dlt-destination.

Creates N managed databases, uploads synthetic Parquet data, loads each table,
then queries back via Arrow IPC and prints per-phase timing stats.

Usage:
    uv run python scripts/load_test.py --workspace-id <id> [options]

Required env vars:
    HOTDATA_API_KEY

Options:
    --databases N     databases to create (default: 5)
    --rows N          rows per table (default: 10000)
    --concurrency N   parallel workers (default: 3)
    --no-query        skip the read/query phase
    --no-cleanup      keep databases after the test
"""

from __future__ import annotations

import argparse
import contextlib
import os
import random
import statistics
import string
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from hotdata_dlt_destination.hotdata_client import HotdataClient

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

CATEGORIES = ["alpha", "beta", "gamma", "delta", "epsilon"]


def _random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def generate_table(rows: int) -> pa.Table:
    rng = random.Random()
    ids = list(range(1, rows + 1))
    names = [_random_string(10) for _ in range(rows)]
    values = [rng.gauss(100.0, 20.0) for _ in range(rows)]
    categories = [rng.choice(CATEGORIES) for _ in range(rows)]
    created_at = [
        datetime.now(UTC).replace(microsecond=rng.randint(0, 999999)).isoformat()
        for _ in range(rows)
    ]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.float64()),
            "category": pa.array(categories, type=pa.string()),
            "created_at": pa.array(created_at, type=pa.string()),
        }
    )


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------


@dataclass
class PhaseResult:
    database: str
    phase: str
    elapsed: float
    rows: int = 0
    error: str | None = None


@dataclass
class Summary:
    phase: str
    results: list[PhaseResult] = field(default_factory=list)

    @property
    def successes(self) -> list[PhaseResult]:
        return [r for r in self.results if r.error is None]

    @property
    def errors(self) -> list[PhaseResult]:
        return [r for r in self.results if r.error is not None]

    def print(self) -> None:
        ok = self.successes
        errs = self.errors
        label = f"[{self.phase}]"
        if not ok:
            print(f"  {label:<18} 0 ok / {len(errs)} errors")
            return
        times = [r.elapsed for r in ok]
        total_rows = sum(r.rows for r in ok)
        elapsed_total = sum(times)
        rps = total_rows / elapsed_total if elapsed_total > 0 else 0
        print(
            f"  {label:<18} n={len(ok):>3}  "
            f"mean={statistics.mean(times):>6.2f}s  "
            f"p50={statistics.median(times):>6.2f}s  "
            f"p95={_percentile(times, 95):>6.2f}s  "
            f"min={min(times):>6.2f}s  max={max(times):>6.2f}s  "
            f"rows={total_rows:>8,}  {rps:>8,.0f} rows/s"
            + (f"  ERRORS={len(errs)}" if errs else "")
        )


def _percentile(data: list[float], pct: int) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (k - lo)


# ---------------------------------------------------------------------------
# Per-database worker
# ---------------------------------------------------------------------------


def run_database_load(
    *,
    db_name: str,
    rows: int,
    api_key: str,
    workspace_id: str,
    api_base_url: str,
    do_query: bool,
) -> list[PhaseResult]:
    results: list[PhaseResult] = []

    client = HotdataClient(
        api_key=api_key,
        workspace_id=workspace_id,
        api_base_url=api_base_url,
        max_retries=3,
        retry_backoff_seconds=1.0,
    )

    try:
        # --- create database ---
        t0 = time.perf_counter()
        try:
            client.ensure_managed_database(
                db_name,
                schema="public",
                tables=["events"],
                create_if_missing=True,
            )
            results.append(PhaseResult(db_name, "create_db", time.perf_counter() - t0))
        except Exception as exc:
            results.append(
                PhaseResult(db_name, "create_db", time.perf_counter() - t0, error=str(exc))
            )
            return results

        # --- generate + write parquet ---
        table = generate_table(rows)
        parquet_path: str | None = None
        t0 = time.perf_counter()
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as fh:
                parquet_path = fh.name
            pq.write_table(table, parquet_path)
            write_elapsed = time.perf_counter() - t0
            results.append(PhaseResult(db_name, "write_parquet", write_elapsed, rows=rows))
        except Exception as exc:
            results.append(
                PhaseResult(db_name, "write_parquet", time.perf_counter() - t0, error=str(exc))
            )
            return results

        # --- upload parquet ---
        t0 = time.perf_counter()
        try:
            upload_id = client.upload_parquet(parquet_path)
            results.append(PhaseResult(db_name, "upload", time.perf_counter() - t0, rows=rows))
        except Exception as exc:
            results.append(PhaseResult(db_name, "upload", time.perf_counter() - t0, error=str(exc)))
            return results
        finally:
            if parquet_path:
                with contextlib.suppress(OSError):
                    os.unlink(parquet_path)

        # --- load managed table ---
        t0 = time.perf_counter()
        try:
            client.load_managed_table(db_name, "events", schema="public", upload_id=upload_id)
            results.append(PhaseResult(db_name, "load", time.perf_counter() - t0, rows=rows))
        except Exception as exc:
            results.append(PhaseResult(db_name, "load", time.perf_counter() - t0, error=str(exc)))
            return results

        # --- query via Arrow IPC ---
        if do_query:
            t0 = time.perf_counter()
            try:
                result_table = client.fetch_table(database=db_name, schema="public", table="events")
                n = len(result_table) if result_table is not None else 0
                results.append(PhaseResult(db_name, "query", time.perf_counter() - t0, rows=n))
            except Exception as exc:
                results.append(
                    PhaseResult(db_name, "query", time.perf_counter() - t0, error=str(exc))
                )

    finally:
        client.close()

    return results


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def delete_databases(
    db_names: list[str],
    *,
    api_key: str,
    workspace_id: str,
    api_base_url: str,
) -> None:
    from hotdata_framework.client import HotdataClient as RuntimeClient

    client = RuntimeClient(api_key, workspace_id, host=api_base_url.rstrip("/"))
    try:
        for name in db_names:
            try:
                db = client.resolve_managed_database(name)
                client.delete_managed_database(db.id)
                print(f"  deleted {name}")
            except Exception as exc:
                print(f"  could not delete {name}: {exc}")
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--workspace-id", required=True, help="Hotdata workspace id")
    parser.add_argument(
        "--databases", type=int, default=5, metavar="N", help="number of databases (default: 5)"
    )
    parser.add_argument(
        "--rows", type=int, default=10_000, metavar="N", help="rows per table (default: 10000)"
    )
    parser.add_argument(
        "--concurrency", type=int, default=3, metavar="N", help="parallel workers (default: 3)"
    )
    parser.add_argument("--no-query", action="store_true", help="skip the query phase")
    parser.add_argument("--no-cleanup", action="store_true", help="keep databases after the test")
    args = parser.parse_args()

    api_key = os.environ["HOTDATA_API_KEY"]
    workspace_id = args.workspace_id
    api_base_url = os.environ.get("HOTDATA_API_BASE_URL", "https://api.hotdata.dev")

    run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    db_names = [f"loadtest_{run_id}_{i:03d}" for i in range(args.databases)]

    phases = ["create_db", "write_parquet", "upload", "load", "query"]
    summaries: dict[str, Summary] = {p: Summary(p) for p in phases}

    print(
        f"\nLoad test  run={run_id}  databases={args.databases}  "
        f"rows={args.rows:,}  concurrency={args.concurrency}  query={'yes' if not args.no_query else 'no'}"
    )
    print("-" * 80)

    wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {
            pool.submit(
                run_database_load,
                db_name=name,
                rows=args.rows,
                api_key=api_key,
                workspace_id=workspace_id,
                api_base_url=api_base_url,
                do_query=not args.no_query,
            ): name
            for name in db_names
        }

        completed = 0
        for future in as_completed(futures):
            name = futures[future]
            completed += 1
            try:
                phase_results = future.result()
            except Exception as exc:
                print(f"  [{completed:>3}/{args.databases}] {name}  FATAL: {exc}")
                continue

            success_phases = [r.phase for r in phase_results if r.error is None]
            error_phases = [r.phase for r in phase_results if r.error is not None]
            status = "ok" if not error_phases else f"ERRORS in {error_phases}"
            print(f"  [{completed:>3}/{args.databases}] {name}  phases={success_phases}  {status}")

            for r in phase_results:
                summaries[r.phase].results.append(r)

    wall_elapsed = time.perf_counter() - wall_start

    print("-" * 80)
    print(f"Wall time: {wall_elapsed:.2f}s\n")
    print("Per-phase stats:")
    for phase in phases:
        s = summaries[phase]
        if s.results:
            s.print()

    print()

    if not args.no_cleanup:
        print("Cleaning up databases...")
        delete_databases(
            db_names,
            api_key=api_key,
            workspace_id=workspace_id,
            api_base_url=api_base_url,
        )
    else:
        print("Skipping cleanup (--no-cleanup). Databases created:")
        for name in db_names:
            print(f"  {name}")


if __name__ == "__main__":
    main()
