#!/usr/bin/env python3
"""
funding_backfill_v2.py — Forward-paginating Binance funding rate backfill.
Paginates using startTime, moving forward from 2019-09-01.
"""

import os, time, requests, psycopg2, psycopg2.extras
import pandas as pd
from datetime import datetime, timezone
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]
BINANCE_BASE = "https://fapi.binance.com"
SLEEP = 0.12
GENESIS_MS = 1567296000000  # 2019-09-01 00:00 UTC


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


def get_symbol_mapping():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT symbol, coingecko_id FROM asset_registry")
    rows = cur.fetchall()
    conn.close()
    return {r["symbol"] + "USDT": {"symbol": r["symbol"], "coingecko_id": r["coingecko_id"]} for r in rows}


def get_binance_perp_symbols():
    resp = requests.get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo", timeout=30)
    resp.raise_for_status()
    return [s["symbol"] for s in resp.json()["symbols"]
            if s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT" and s["status"] == "TRADING"]


def backfill_symbol(binance_symbol, our_symbol, coingecko_id):
    """Forward-paginate: start from genesis, move forward in 1000-record chunks."""
    all_rows = []
    start_time = GENESIS_MS
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    while start_time < now_ms:
        for attempt in range(3):
            try:
                resp = requests.get(
                    f"{BINANCE_BASE}/fapi/v1/fundingRate",
                    params={"symbol": binance_symbol, "startTime": start_time, "limit": 1000},
                    timeout=30
                )
                if resp.status_code == 429:
                    time.sleep(10 * (attempt + 1))
                    continue
                if resp.status_code == 400:
                    return all_rows
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.RequestException:
                if attempt < 2:
                    time.sleep(5)
                else:
                    return all_rows
        else:
            break

        if not data:
            break

        all_rows.extend(data)

        # Move start_time to just after the last record in this batch
        last_ts = data[-1]["fundingTime"]
        start_time = last_ts + 1

        # If fewer than 1000 records, we've reached the end
        if len(data) < 1000:
            break

        time.sleep(SLEEP)

    return all_rows


def main():
    print("━" * 60)
    print("  BINANCE FUNDING RATE — FULL FORWARD BACKFILL v2")
    print("━" * 60)

    binance_symbols = get_binance_perp_symbols()
    symbol_map = get_symbol_mapping()
    print(f"  {len(binance_symbols)} Binance perp symbols, matching to registry...")

    to_process = [(bs, symbol_map[bs]["symbol"], symbol_map[bs]["coingecko_id"])
                  for bs in binance_symbols if bs in symbol_map]
    print(f"  {len(to_process)} symbols to backfill\n")

    total_inserted = 0
    for i, (bs, sym, cgid) in enumerate(to_process):
        print(f"  [{i+1}/{len(to_process)}] {sym:8s}", end="", flush=True)

        raw = backfill_symbol(bs, sym, cgid)
        if not raw:
            print(" → 0 rows (no data)")
            continue

        df = pd.DataFrame(raw)
        df["timestamp"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
        df["funding_rate"] = df["fundingRate"].astype(float)
        df["coingecko_id"] = cgid
        df["symbol"] = sym
        df["exchange"] = "binance"
        df["source"] = "binance"
        df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        df = df[["timestamp", "coingecko_id", "symbol", "exchange", "funding_rate", "source", "ingested_at"]]
        df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

        n = bulk_upsert("funding_8h", df, ["timestamp", "coingecko_id", "exchange"])
        total_inserted += n
        print(f" → {len(raw)} fetched, +{n} new rows ({df['timestamp'].min().date()} to {df['timestamp'].max().date()})")

    print(f"\n{'━' * 60}")
    print(f"  DONE — {total_inserted} total new rows inserted")
    print(f"{'━' * 60}")


if __name__ == "__main__":
    main()
