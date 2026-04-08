#!/usr/bin/env python3
"""
btc_onchain_backfill.py
───────────────────────
Pulls BTC hash rate and active addresses from CoinMetrics Community API.
Stores in onchain_daily table. Run once to backfill, then add to daily cron.

Run:
  DATABASE_URL="postgresql://..." python3 btc_onchain_backfill.py
"""

import os, requests, psycopg2, psycopg2.extras
import pandas as pd
from datetime import datetime, timezone, timedelta
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]
CM_BASE = "https://community-api.coinmetrics.io/v4"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def bulk_upsert(table, df, conflict_cols):
    if df.empty: return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cols = list(df.columns)
        cur.execute(f"CREATE TEMP TABLE _tmp (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "_tmp", columns=cols, null="\\N")
        col_str = ", ".join(cols)
        conflict_str = ", ".join(conflict_cols)
        cur.execute(f"INSERT INTO {table} ({col_str}) SELECT {col_str} FROM _tmp ON CONFLICT ({conflict_str}) DO NOTHING")
        n = cur.rowcount
        conn.commit()
        return n
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("━" * 60)
    print("  BTC ON-CHAIN DATA BACKFILL (CoinMetrics)")
    print("━" * 60)

    # Create table if not exists
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS onchain_daily (
            timestamp TIMESTAMPTZ NOT NULL,
            asset TEXT NOT NULL,
            metric TEXT NOT NULL,
            value DOUBLE PRECISION,
            source TEXT,
            ingested_at TIMESTAMPTZ,
            UNIQUE (timestamp, asset, metric)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] Table onchain_daily ready")

    # Check existing data
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(timestamp) FROM onchain_daily WHERE asset = 'BTC'")
    last = cur.fetchone()[0]
    conn.close()

    if last:
        start = (last.date() + timedelta(days=1)).isoformat()
        print(f"  Existing data up to {last.date()}, starting from {start}")
    else:
        start = "2009-01-03"  # BTC genesis
        print(f"  No existing data, full backfill from {start}")

    end = datetime.now(timezone.utc).date().isoformat()

    # Fetch from CoinMetrics Community API
    metrics = ["HashRate", "AdrActCnt"]
    all_rows = []

    for metric in metrics:
        print(f"\nFetching {metric}...")
        page_token = None
        total = 0

        while True:
            params = {
                "assets": "btc",
                "metrics": metric,
                "start_time": start,
                "end_time": end,
                "frequency": "1d",
                "page_size": 10000,
            }
            if page_token:
                params["next_page_token"] = page_token

            resp = requests.get(f"{CM_BASE}/timeseries/asset-metrics", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            rows = data.get("data", [])
            if not rows:
                break

            for r in rows:
                val = r.get(metric)
                if val is not None:
                    all_rows.append({
                        "timestamp": r["time"],
                        "asset": "BTC",
                        "metric": metric,
                        "value": float(val),
                        "source": "coinmetrics",
                        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    })

            total += len(rows)
            print(f"  Fetched {total} rows...", end="\r")

            page_token = data.get("next_page_token")
            if not page_token:
                break

        print(f"  {total} total rows for {metric}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        n = bulk_upsert("onchain_daily", df, ["timestamp", "asset", "metric"])
        print(f"\n  +{n} rows inserted")
    else:
        print("\n  No new data")

    print("\n" + "━" * 60)
    print("  DONE")
    print("━" * 60)


if __name__ == "__main__":
    main()
