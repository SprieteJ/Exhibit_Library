#!/usr/bin/env python3
"""
dvol_backfill.py
────────────────
Pulls full historical DVOL (Deribit Implied Volatility Index) data
for BTC and ETH from the Deribit public API.

DVOL data available since ~March 2021.
No API key required — this is a public endpoint.

The endpoint: public/get_volatility_index_data
Returns OHLC candles for the DVOL index.

Usage:
    python dvol_backfill.py                    # full backfill
    python dvol_backfill.py --incremental      # only fetch new data

Environment:
    DATABASE_URL  — Postgres connection string
"""

import os
import sys
import time
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timezone, timedelta
from io import StringIO

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DATABASE_URL = os.environ["DATABASE_URL"]
DERIBIT_BASE = "https://www.deribit.com/api/v2"
CURRENCIES   = ["BTC", "ETH"]
RESOLUTION   = "1D"   # daily candles (options: 1, 60, 3600, 43200, 1D)
SOURCE       = "deribit"

# DVOL launched around March 2021
DEFAULT_START = datetime(2021, 3, 1, tzinfo=timezone.utc)

# Deribit returns max ~700 candles per request, so we paginate in 600-day chunks
CHUNK_DAYS   = 600
SLEEP        = 0.3


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def get_latest_dvol(currency: str) -> datetime | None:
    """Get the most recent DVOL timestamp for a currency."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM dvol_daily WHERE currency = %s", (currency,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def bulk_insert_dvol(df: pd.DataFrame):
    """Insert DVOL rows into Postgres, skipping duplicates."""
    if df.empty:
        return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cols = list(df.columns)
        col_str = ", ".join(cols)

        cur.execute("CREATE TEMP TABLE _tmp_dvol (LIKE dvol_daily INCLUDING DEFAULTS) ON COMMIT DROP")

        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "_tmp_dvol", columns=cols, null="\\N")

        cur.execute(f"""
            INSERT INTO dvol_daily ({col_str})
            SELECT {col_str} FROM _tmp_dvol
            ON CONFLICT (timestamp, currency) DO NOTHING
        """)

        inserted = cur.rowcount
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# DERIBIT API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_dvol_chunk(currency: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    """
    Fetch DVOL candles from Deribit.
    Endpoint: public/get_volatility_index_data
    Returns: {data: [[timestamp, open, high, low, close], ...], continuation: ...}
    """
    url = f"{DERIBIT_BASE}/public/get_volatility_index_data"
    params = {
        "currency": currency,
        "start_timestamp": start_ms,
        "end_timestamp": end_ms,
        "resolution": RESOLUTION,
    }

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            result = r.json().get("result", {})
            data = result.get("data", [])

            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=["timestamp_ms", "open", "high", "low", "close"])
            df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
            df = df[["timestamp", "open", "high", "low", "close"]]
            df = df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
            return df

        except Exception as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                raise

    return pd.DataFrame()


def fetch_dvol_full(currency: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch DVOL data in chunks, paginating through the full date range."""
    all_dfs = []
    current = start

    while current < end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), end)
        start_ms = int(current.timestamp() * 1000)
        end_ms   = int(chunk_end.timestamp() * 1000)

        df = fetch_dvol_chunk(currency, start_ms, end_ms)
        if not df.empty:
            all_dfs.append(df)
            print(f"    {current.date()} → {chunk_end.date()}: {len(df)} candles")
        else:
            print(f"    {current.date()} → {chunk_end.date()}: no data")

        current = chunk_end
        time.sleep(SLEEP)

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    return combined.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    incremental = "--incremental" in sys.argv
    now_utc = datetime.now(timezone.utc)
    ingested_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\n{'━' * 60}")
    print(f"  DVOL {'UPDATE' if incremental else 'BACKFILL'}")
    print(f"  {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'━' * 60}\n")

    total_inserted = 0

    for currency in CURRENCIES:
        print(f"[{currency}] ", end="")

        if incremental:
            last = get_latest_dvol(currency)
            if last:
                start = last + timedelta(hours=1)  # start just after last known
                gap_days = (now_utc - start).days
                if gap_days <= 0:
                    print("up to date")
                    continue
                print(f"{gap_days}d gap")
            else:
                start = DEFAULT_START
                print("no existing data — full fetch")
        else:
            start = DEFAULT_START
            print("full backfill")

        df = fetch_dvol_full(currency, start, now_utc)

        if df.empty:
            print(f"  no data returned\n")
            continue

        # Drop today's partial candle
        df = df[df["timestamp"].dt.date < now_utc.date()]

        df["currency"]    = currency
        df["source"]      = SOURCE
        df["ingested_at"] = ingested_at
        df = df[["timestamp", "currency", "open", "high", "low", "close", "source", "ingested_at"]]

        n = bulk_insert_dvol(df)
        total_inserted += n
        print(f"  [{currency}] +{n} rows inserted\n")

    print(f"{'━' * 60}")
    print(f"  DONE — {total_inserted} total rows inserted")
    print(f"{'━' * 60}\n")


if __name__ == "__main__":
    main()
