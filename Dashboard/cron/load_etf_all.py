#!/usr/bin/env python3
"""Load BTC + ETH ETF flow CSVs into etf_flows_daily."""

import os, psycopg2, psycopg2.extras, pandas as pd
from datetime import datetime, timezone
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def load_csv(path, asset):
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["source"] = "farside"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n  [{asset}] {len(df)} records, {df['timestamp'].min().date()} to {df['timestamp'].max().date()}")
    print(f"    Tickers: {sorted(df['ticker'].unique())}")

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TEMP TABLE _tmp (LIKE etf_flows_daily INCLUDING DEFAULTS) ON COMMIT DROP")
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cols = list(df.columns)
        cur.copy_from(buf, "_tmp", columns=cols, null="\\N")
        col_str = ", ".join(cols)
        update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in ["flow_usd_m", "source", "ingested_at"])
        cur.execute(f"INSERT INTO etf_flows_daily ({col_str}) SELECT {col_str} FROM _tmp ON CONFLICT (timestamp, ticker, asset) DO UPDATE SET {update_str}")
        n = cur.rowcount
        conn.commit()
        print(f"    +{n} rows upserted")
        return n
    except Exception as e:
        conn.rollback()
        print(f"    Error: {e}")
        return 0
    finally:
        conn.close()

print("━" * 50)
print("  LOADING ETF FLOWS")
print("━" * 50)

total = 0
for csv_file, asset in [("btc_etf_flows.csv", "BTC"), ("eth_etf_flows.csv", "ETH")]:
    if os.path.exists(csv_file):
        total += load_csv(csv_file, asset)
    else:
        print(f"\n  [{asset}] {csv_file} not found, skipping")

print(f"\n{'━' * 50}")
print(f"  DONE — {total} total rows")
print(f"{'━' * 50}")
