"""
storage.py
----------
Handles all CSV read/write operations across the data lake.
Designed so every write is idempotent and Postgres-migration-ready.

Key behaviours:
  - Auto-creates the target file + header if it doesn't exist
  - Deduplicates on primary keys before appending
  - Adds ingested_at and source columns if missing
  - Writes ISO 8601 UTC timestamps throughout
  - Path routing is centralised here — scripts just call write(table, df)

Postgres migration path:
  Each table name maps 1:1 to a CSV file which maps 1:1 to a DB table.
  The migrate/ scripts use these same TABLE_PATHS to COPY into Postgres.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ── Data root ────────────────────────────────────────────────────────────────
DATA_ROOT = Path(__file__).resolve().parents[2]

# ── Table → file path mapping ─────────────────────────────────────────────────
# Add new tables here. The migrate scripts read this map.
TABLE_PATHS: dict[str, Path] = {
    # ── Price ──────────────────────────────────────────────────────────────
    "price_daily":              DATA_ROOT / "price"          / "daily"        / "price_daily.csv",
    "price_hourly":             DATA_ROOT / "price"          / "hourly"       / "price_hourly.csv",

    # ── Market cap ─────────────────────────────────────────────────────────
    "marketcap_daily":          DATA_ROOT / "marketcap"      / "daily"        / "marketcap_daily.csv",
    "marketcap_hourly":         DATA_ROOT / "marketcap"      / "hourly"       / "marketcap_hourly.csv",

    # ── Funding rates (exchange-level) ─────────────────────────────────────
    "funding_8h":               DATA_ROOT / "funding"        / "8h"           / "funding_8h.csv",
    "funding_daily":            DATA_ROOT / "funding"        / "daily"        / "funding_daily.csv",

    # ── Open interest (exchange-level) ─────────────────────────────────────
    "open_interest_daily":      DATA_ROOT / "open_interest"  / "daily"        / "oi_daily.csv",
    "open_interest_hourly":     DATA_ROOT / "open_interest"  / "hourly"       / "oi_hourly.csv",

    # ── Liquidations (exchange-level) ──────────────────────────────────────
    "liquidations":             DATA_ROOT / "derivatives"    / "liquidations" / "liquidations.csv",

    # ── Long/short ratio (exchange-level) ──────────────────────────────────
    "long_short_ratio":         DATA_ROOT / "derivatives"    / "long_short"   / "long_short_ratio.csv",

    # ── Options ────────────────────────────────────────────────────────────
    "options_snapshot":         DATA_ROOT / "options"        / "snapshots"    / "vol_surface.csv",

    # ── Volume ─────────────────────────────────────────────────────────────────
    "volume_daily":             DATA_ROOT / "volume"         / "daily"        / "volume_daily.csv",
    "volume_hourly":            DATA_ROOT / "volume"         / "hourly"       / "volume_hourly.csv",

    # ── Macro ──────────────────────────────────────────────────────────────────
    "macro_daily":              DATA_ROOT / "macro"          / "daily"        / "macro_daily.csv",
    "macro_hourly":             DATA_ROOT / "macro"          / "hourly"       / "macro_hourly.csv",
}

# ── Primary keys per table (used for dedup) ───────────────────────────────────
TABLE_KEYS: dict[str, list[str]] = {
    "price_daily":              ["timestamp", "coingecko_id"],
    "price_hourly":             ["timestamp", "coingecko_id"],
    "marketcap_daily":          ["timestamp", "coingecko_id"],
    "marketcap_hourly":         ["timestamp", "coingecko_id"],
    "funding_8h":               ["timestamp", "coingecko_id", "exchange"],
    "funding_daily":            ["timestamp", "coingecko_id", "exchange"],
    "open_interest_daily":      ["timestamp", "coingecko_id", "exchange"],
    "open_interest_hourly":     ["timestamp", "coingecko_id", "exchange"],
    "liquidations":             ["timestamp", "coingecko_id", "exchange", "side"],
    "long_short_ratio":         ["timestamp", "coingecko_id", "exchange", "period"],
    "options_snapshot":         ["snapshot_date", "coingecko_id", "expiry", "strike"],
    "volume_daily":             ["timestamp", "coingecko_id"],
    "volume_hourly":            ["timestamp", "coingecko_id"],
    "macro_daily":              ["timestamp", "ticker"],
    "macro_hourly":             ["timestamp", "ticker"],
}


# ── Core write function ───────────────────────────────────────────────────────
def write(
    table: str,
    df: pd.DataFrame,
    source: str | None = None,
) -> int:
    """
    Append df to the target table CSV, deduplicating on primary keys.

    Parameters
    ----------
    table   : one of TABLE_PATHS keys
    df      : DataFrame to append
    source  : e.g. "coingecko", "binance", "bybit" — added as column

    Returns
    -------
    rows_written : int
    """
    if table not in TABLE_PATHS:
        raise ValueError(f"Unknown table '{table}'. Options: {list(TABLE_PATHS)}")

    path = TABLE_PATHS[table]
    keys = TABLE_KEYS[table]

    # ── Enrich with audit columns ─────────────────────────────────────────────
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df.copy()
    if source and "source" not in df.columns:
        df["source"] = source
    if "ingested_at" not in df.columns:
        df["ingested_at"] = now_utc

    # ── Ensure timestamp is ISO 8601 UTC string ───────────────────────────────
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    # ── Load existing, dedup, append ─────────────────────────────────────────
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, df], ignore_index=True)
        valid_keys = [k for k in keys if k in combined.columns]
        combined = combined.drop_duplicates(subset=valid_keys, keep="last")
    else:
        combined = df

    combined.to_csv(path, index=False)
    rows_written = len(df)
    print(f"[storage] {table}: +{rows_written} rows → {path.relative_to(DATA_ROOT)}")
    return rows_written


# ── Read function ─────────────────────────────────────────────────────────────
def read(
    table: str,
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """
    Read a table CSV with optional filtering.

    Parameters
    ----------
    table   : one of TABLE_PATHS keys
    symbols : list of coingecko_ids to filter to (None = all)
    start   : ISO date string, e.g. "2025-01-01"
    end     : ISO date string, e.g. "2025-12-31"
    """
    path = TABLE_PATHS[table]
    if not path.exists():
        raise FileNotFoundError(f"No data yet for table '{table}' at {path}")

    df = pd.read_csv(path)

    if symbols:
        df = df[df["coingecko_id"].isin(symbols)]

    if "timestamp" in df.columns and (start or end):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        if start:
            df = df[df["timestamp"] >= pd.Timestamp(start, tz="UTC")]
        if end:
            df = df[df["timestamp"] <= pd.Timestamp(end, tz="UTC")]

    return df.reset_index(drop=True)


# ── Table info ────────────────────────────────────────────────────────────────
def table_info() -> pd.DataFrame:
    """Summary of all tables: path, exists, row count, size."""
    rows = []
    for name, path in TABLE_PATHS.items():
        exists = path.exists()
        if exists:
            size_kb = round(os.path.getsize(path) / 1024, 1)
            nrows = sum(1 for _ in open(path)) - 1
        else:
            size_kb = 0
            nrows = 0
        rows.append({
            "table":   name,
            "exists":  exists,
            "rows":    nrows,
            "size_kb": size_kb,
            "path":    str(path.relative_to(DATA_ROOT)),
        })
    return pd.DataFrame(rows)
