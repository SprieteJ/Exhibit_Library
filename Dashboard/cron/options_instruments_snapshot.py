#!/usr/bin/env python3
"""
options_instruments_snapshot.py
───────────────────────────────
Pulls per-instrument options data from Deribit for BTC + ETH.
Stores every contract with strike, expiry, OI, volume, IV.

Run:
  DATABASE_URL="postgresql://..." python3 options_instruments_snapshot.py
"""

import os, requests, psycopg2, psycopg2.extras
import pandas as pd
from datetime import datetime, timezone
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]
DERIBIT_BASE = "https://www.deribit.com/api/v2/public"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def create_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS options_instruments_daily (
            timestamp        TIMESTAMPTZ NOT NULL,
            currency         TEXT NOT NULL,
            instrument_name  TEXT NOT NULL,
            expiry_date      TEXT,
            strike           DOUBLE PRECISION,
            option_type      TEXT,
            open_interest    DOUBLE PRECISION,
            volume           DOUBLE PRECISION,
            volume_usd       DOUBLE PRECISION,
            mark_iv          DOUBLE PRECISION,
            mark_price       DOUBLE PRECISION,
            bid_price        DOUBLE PRECISION,
            ask_price        DOUBLE PRECISION,
            underlying_price DOUBLE PRECISION,
            source           TEXT,
            ingested_at      TIMESTAMPTZ,
            UNIQUE (timestamp, instrument_name)
        )
    """)
    # Index for common queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_oid_currency_ts 
        ON options_instruments_daily (currency, timestamp)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_oid_expiry 
        ON options_instruments_daily (expiry_date, timestamp)
    """)
    conn.commit()
    conn.close()
    print("[OK] Table options_instruments_daily ready")


def fetch_and_store(currency):
    """Fetch all option instruments for a currency and store."""
    print(f"\n  [{currency}] Fetching...", end=" ")

    # Get book summaries
    resp = requests.get(
        f"{DERIBIT_BASE}/get_book_summary_by_currency",
        params={"currency": currency, "kind": "option"},
        timeout=30
    )
    resp.raise_for_status()
    data = resp.json().get("result", [])

    if not data:
        print("no data")
        return 0

    # Get underlying price
    presp = requests.get(
        f"{DERIBIT_BASE}/get_index_price",
        params={"index_name": f"{currency.lower()}_usd"},
        timeout=15
    )
    price = presp.json()["result"]["index_price"]

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    ingested = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = []
    for item in data:
        name = item.get("instrument_name", "")
        parts = name.split("-")
        if len(parts) != 4:
            continue

        expiry = parts[1]
        strike = float(parts[2])
        opt_type = "put" if parts[3] == "P" else "call"

        rows.append({
            "timestamp": today,
            "currency": currency,
            "instrument_name": name,
            "expiry_date": expiry,
            "strike": strike,
            "option_type": opt_type,
            "open_interest": float(item.get("open_interest") or 0),
            "volume": float(item.get("volume") or 0),
            "volume_usd": float(item.get("volume_usd") or 0),
            "mark_iv": float(item["mark_iv"]) if item.get("mark_iv") else None,
            "mark_price": float(item["mark_price"]) if item.get("mark_price") else None,
            "bid_price": float(item["bid_price"]) if item.get("bid_price") else None,
            "ask_price": float(item["ask_price"]) if item.get("ask_price") else None,
            "underlying_price": price,
            "source": "deribit",
            "ingested_at": ingested,
        })

    if not rows:
        print("0 instruments")
        return 0

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Bulk upsert
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TEMP TABLE _tmp (LIKE options_instruments_daily INCLUDING DEFAULTS) ON COMMIT DROP")
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cols = list(df.columns)
        cur.copy_from(buf, "_tmp", columns=cols, null="\\N")
        col_str = ", ".join(cols)
        update_cols = ["open_interest", "volume", "volume_usd", "mark_iv", "mark_price",
                       "bid_price", "ask_price", "underlying_price", "source", "ingested_at"]
        update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        cur.execute(f"INSERT INTO options_instruments_daily ({col_str}) SELECT {col_str} FROM _tmp ON CONFLICT (timestamp, instrument_name) DO UPDATE SET {update_str}")
        n = cur.rowcount
        conn.commit()
        print(f"{len(rows)} instruments, +{n} rows, underlying: ${price:,.0f}")
        return n
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        return 0
    finally:
        conn.close()


def main():
    print("━" * 60)
    print("  OPTIONS INSTRUMENTS SNAPSHOT (Deribit)")
    print("━" * 60)

    create_table()

    total = 0
    for currency in ["BTC", "ETH"]:
        total += fetch_and_store(currency)

    print(f"\n{'━' * 60}")
    print(f"  DONE — {total} total rows")
    print(f"{'━' * 60}")


if __name__ == "__main__":
    main()
