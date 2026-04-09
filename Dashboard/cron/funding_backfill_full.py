#!/usr/bin/env python3
"""
funding_backfill_full.py
────────────────────────
Pulls FULL funding rate history from Binance Futures API for all perp symbols.
Paginates in 1000-record chunks back to October 2019.

WARNING: This will take a while — hundreds of symbols × years of data.
Run from EU (Binance blocks US IPs) or use Railway.

Run:
  DATABASE_URL="postgresql://..." python3 funding_backfill_full.py
"""

import os, time, requests, psycopg2, psycopg2.extras
import pandas as pd
from datetime import datetime, timezone
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]
BINANCE_BASE = "https://fapi.binance.com"
SLEEP = 0.15  # rate limit: ~6 req/s

# Map Binance symbols to coingecko_id and our symbol
# We'll build this dynamically from asset_registry


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


def get_binance_perp_symbols():
    """Get all active USDT perpetual symbols from Binance."""
    print("Fetching Binance perpetual symbols...")
    resp = requests.get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo", timeout=30)
    resp.raise_for_status()
    symbols = []
    for s in resp.json()["symbols"]:
        if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT" and s["status"] == "TRADING":
            symbols.append(s["symbol"])
    print(f"  Found {len(symbols)} active USDT perpetual symbols")
    return symbols


def get_symbol_mapping():
    """Build mapping from Binance symbol (e.g. BTCUSDT) to our symbol + coingecko_id."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT symbol, coingecko_id FROM asset_registry")
    rows = cur.fetchall()
    conn.close()

    mapping = {}
    for r in rows:
        binance_sym = r["symbol"] + "USDT"
        mapping[binance_sym] = {"symbol": r["symbol"], "coingecko_id": r["coingecko_id"]}
    return mapping


def get_existing_min_timestamps():
    """Get the earliest timestamp per symbol we already have from Binance."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT symbol, MIN(timestamp) as min_ts
        FROM funding_8h
        WHERE exchange = 'binance'
        GROUP BY symbol
    """)
    rows = cur.fetchall()
    conn.close()
    return {r["symbol"]: r["min_ts"] for r in rows}


def fetch_funding_history(binance_symbol, end_time=None):
    """Fetch one page of funding rate history (up to 1000 records, going backwards)."""
    params = {"symbol": binance_symbol, "limit": 1000}
    if end_time:
        params["endTime"] = end_time

    for attempt in range(3):
        try:
            resp = requests.get(f"{BINANCE_BASE}/fapi/v1/fundingRate", params=params, timeout=30)
            if resp.status_code == 429:
                time.sleep(10 * (attempt + 1))
                continue
            if resp.status_code == 400:
                return []
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                return []
    return []


def backfill_symbol(binance_symbol, our_symbol, coingecko_id, existing_min_ts=None):
    """Paginate backwards through full funding history for one symbol."""
    all_rows = []
    end_time = None

    # If we have existing data, start from before the earliest we have
    if existing_min_ts:
        end_time = int(existing_min_ts.timestamp() * 1000) - 1

    while True:
        data = fetch_funding_history(binance_symbol, end_time)
        if not data:
            break

        all_rows.extend(data)

        # The API returns oldest first, so the first record is the earliest
        earliest_ts = data[0]["fundingTime"]

        # If we got fewer than 1000, we've reached the beginning
        if len(data) < 1000:
            break

        # Move end_time to just before the earliest record in this batch
        end_time = earliest_ts - 1
        time.sleep(SLEEP)

    if not all_rows:
        return 0

    df = pd.DataFrame(all_rows)
    df["timestamp"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["funding_rate"] = df["fundingRate"].astype(float)
    df["coingecko_id"] = coingecko_id
    df["symbol"] = our_symbol
    df["exchange"] = "binance"
    df["source"] = "binance"
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df[["timestamp", "coingecko_id", "symbol", "exchange", "funding_rate", "source", "ingested_at"]]
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    n = bulk_upsert("funding_8h", df, ["timestamp", "coingecko_id", "exchange"])
    return n


def main():
    print("━" * 60)
    print("  BINANCE FUNDING RATE — FULL HISTORY BACKFILL")
    print("━" * 60)

    binance_symbols = get_binance_perp_symbols()
    symbol_map = get_symbol_mapping()
    existing = get_existing_min_timestamps()

    # Only backfill symbols we have in our registry
    to_process = []
    for bs in binance_symbols:
        if bs in symbol_map:
            to_process.append((bs, symbol_map[bs]["symbol"], symbol_map[bs]["coingecko_id"]))

    print(f"\n{len(to_process)} symbols to backfill (matched to registry)")
    total_inserted = 0

    for i, (bs, sym, cgid) in enumerate(to_process):
        existing_min = existing.get(sym)
        status = f"  [{i+1}/{len(to_process)}] {sym:8s}"

        if existing_min:
            status += f" (have from {existing_min.date()}, backfilling earlier)"
        else:
            status += " (no data, full backfill)"

        print(status, end="", flush=True)
        n = backfill_symbol(bs, sym, cgid, existing_min)
        total_inserted += n
        print(f" → +{n} rows")

    print(f"\n{'━' * 60}")
    print(f"  DONE — {total_inserted} total rows inserted")
    print(f"{'━' * 60}")


if __name__ == "__main__":
    main()
