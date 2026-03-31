#!/usr/bin/env python3
"""
total_mcap_backfill.py
──────────────────────
Pulls total crypto market cap history from CoinGecko Pro API
and stores it in a new table `total_marketcap_daily`.

This gives the REAL total crypto market cap (not just our 584 assets).
Used for accurate BTC dominance calculation.

Run:
  DATABASE_URL="..." COINGECKO_API_KEY="..." python3 total_mcap_backfill.py
"""

import os, time, requests, pandas as pd, psycopg2
from datetime import datetime, timezone
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]
CG_API_KEY   = os.environ["COINGECKO_API_KEY"]
CG_BASE      = "https://pro-api.coingecko.com/api/v3"

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
    print("  TOTAL CRYPTO MARKET CAP BACKFILL")
    print("━" * 60)

    # Create table if not exists
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS total_marketcap_daily (
            timestamp TIMESTAMPTZ NOT NULL,
            total_mcap_usd DOUBLE PRECISION,
            source TEXT,
            ingested_at TIMESTAMPTZ,
            UNIQUE (timestamp)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] Table total_marketcap_daily ready")

    # CoinGecko /global/market_cap_chart?days=max
    # Returns {market_cap_chart: {total: [[ts_ms, value], ...]}}
    headers = {"Accept": "application/json", "x-cg-pro-api-key": CG_API_KEY}
    
    print("\nFetching from CoinGecko /global/market_cap_chart?days=max ...")
    r = requests.get(f"{CG_BASE}/global/market_cap_chart", 
                     params={"days": "max", "vs_currency": "usd"},
                     headers=headers, timeout=60)
    r.raise_for_status()
    data = r.json()
    
    # The response structure: {"market_cap_chart": {"total": [[ts_ms, value], ...]}}
    total_data = data.get("market_cap_chart", {}).get("total", [])
    
    if not total_data:
        # Try alternative structure
        total_data = data.get("total_market_cap", [])
    
    if not total_data:
        print(f"  Unexpected response structure. Keys: {list(data.keys())}")
        # Try to find the data
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0:
                print(f"  Found list at key '{k}' with {len(v)} items")
                if isinstance(v[0], list) and len(v[0]) == 2:
                    total_data = v
                    break
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, list) and len(v2) > 0:
                        print(f"  Found list at key '{k}.{k2}' with {len(v2)} items")
                        if isinstance(v2[0], list) and len(v2[0]) == 2:
                            total_data = v2
                            break
        if not total_data:
            print("  Could not find market cap data. Printing first 500 chars of response:")
            import json
            print(json.dumps(data, indent=2)[:500])
            return
    
    print(f"  Got {len(total_data)} data points")
    
    df = pd.DataFrame(total_data, columns=["ts_ms", "total_mcap_usd"])
    df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.normalize()
    
    # Drop today's partial
    today = datetime.now(timezone.utc).date()
    df = df[df["timestamp"].dt.date < today]
    df = df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
    
    df["source"]      = "coingecko"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df[["timestamp", "total_mcap_usd", "source", "ingested_at"]]
    
    n = bulk_upsert("total_marketcap_daily", df, ["timestamp"])
    print(f"  +{n} rows inserted")
    print(f"  Range: {df['timestamp'].min()} → {df['timestamp'].max()}")
    
    print("\n" + "━" * 60)
    print("  DONE")
    print("━" * 60)

if __name__ == "__main__":
    main()
