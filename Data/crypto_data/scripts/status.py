#!/usr/bin/env python3
"""
status.py
---------
Shows a summary of all data currently in the lake:
  - Table name
  - File size
  - Row count
  - Earliest date
  - Latest date
  - Unique assets
  - Granularity

Run anytime to check what you have:
    python3 scripts/status.py
"""

import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.utils.storage import TABLE_PATHS


def infer_granularity(name: str) -> str:
    if "hourly" in name or "_1h" in name:
        return "1h"
    if "daily" in name or "_1d" in name:
        return "1d"
    if "8h" in name:
        return "8h"
    if "snapshot" in name:
        return "snapshot"
    return "unknown"


def summarise_table(name: str, path: Path) -> dict:
    if not path.exists():
        return {
            "table":       name,
            "granularity": infer_granularity(name),
            "exists":      False,
            "size_mb":     0,
            "rows":        0,
            "assets":      0,
            "from":        "—",
            "to":          "—",
        }

    size_mb = round(path.stat().st_size / 1024 / 1024, 2)

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return {
            "table":       name,
            "granularity": infer_granularity(name),
            "exists":      True,
            "size_mb":     size_mb,
            "rows":        "error",
            "assets":      "error",
            "from":        str(e),
            "to":          "—",
        }

    # Timestamp column
    ts_col = next((c for c in ["timestamp", "snapshot_date", "date"] if c in df.columns), None)
    if ts_col:
        ts = pd.to_datetime(df[ts_col], errors="coerce")
        date_from = ts.min().strftime("%Y-%m-%d") if not ts.isna().all() else "—"
        date_to   = ts.max().strftime("%Y-%m-%d") if not ts.isna().all() else "—"
    else:
        date_from = date_to = "—"

    # Asset count
    id_col = next((c for c in ["coingecko_id", "ticker", "symbol"] if c in df.columns), None)
    n_assets = df[id_col].nunique() if id_col else "—"

    return {
        "table":       name,
        "granularity": infer_granularity(name),
        "exists":      True,
        "size_mb":     size_mb,
        "rows":        len(df),
        "assets":      n_assets,
        "from":        date_from,
        "to":          date_to,
    }


def main():
    rows = [summarise_table(name, path) for name, path in TABLE_PATHS.items()]
    df = pd.DataFrame(rows)

    # Split into populated and empty
    populated = df[df["exists"] & (df["rows"] != 0)].copy()
    empty     = df[~df["exists"] | (df["rows"] == 0)].copy()

    pd.set_option("display.max_rows", 100)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 160)
    pd.set_option("display.colheader_justify", "left")

    print("\n" + "═" * 110)
    print("  DATA LAKE STATUS")
    print("═" * 110)

    if not populated.empty:
        print(populated[[
            "table", "granularity", "size_mb", "rows", "assets", "from", "to"
        ]].to_string(index=False))

    if not empty.empty:
        print(f"\n{'─' * 110}")
        print("  NOT YET POPULATED:")
        for _, r in empty.iterrows():
            print(f"  • {r['table']}")

    print("═" * 110)
    total_mb = df["size_mb"].sum()
    total_rows = pd.to_numeric(df["rows"], errors="coerce").sum()
    print(f"  Total size: {total_mb:.1f} MB  |  Total rows: {int(total_rows):,}")
    print("═" * 110 + "\n")


if __name__ == "__main__":
    main()
