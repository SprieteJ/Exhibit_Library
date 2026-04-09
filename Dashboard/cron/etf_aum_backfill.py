#!/usr/bin/env python3
"""
etf_aum_backfill.py
───────────────────
Pulls total net assets (AUM) for all BTC and ETH spot ETFs from yfinance.
Uses market cap as proxy (yfinance provides it for ETFs).

Run:
  DATABASE_URL="postgresql://..." python3 etf_aum_backfill.py
"""

import os, psycopg2, psycopg2.extras
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone, timedelta
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]

BTC_ETFS = ["IBIT", "FBTC", "BITB", "ARKB", "BTCO", "EZBC", "BRRR", "HODL", "BTCW", "GBTC"]
ETH_ETFS = ["ETHA", "FETH", "ETHW", "ETHV", "QETH", "EZET", "ETHE"]

ASSET_MAP = {}
for t in BTC_ETFS: ASSET_MAP[t] = "BTC"
for t in ETH_ETFS: ASSET_MAP[t] = "ETH"

ALL_TICKERS = BTC_ETFS + ETH_ETFS


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def main():
    print("━" * 60)
    print("  ETF AUM BACKFILL (yfinance)")
    print("━" * 60)

    # Create table
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etf_aum_daily (
            timestamp    TIMESTAMPTZ NOT NULL,
            ticker       TEXT NOT NULL,
            asset        TEXT NOT NULL,
            aum_usd      DOUBLE PRECISION,
            close_price  DOUBLE PRECISION,
            shares_out   DOUBLE PRECISION,
            source       TEXT,
            ingested_at  TIMESTAMPTZ,
            UNIQUE (timestamp, ticker)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] Table etf_aum_daily ready")

    # Check existing data
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT ticker, MAX(timestamp)::date FROM etf_aum_daily GROUP BY ticker")
        existing = {r[0]: r[1] for r in cur.fetchall()}
    except:
        existing = {}
    conn.close()

    now = datetime.now(timezone.utc)
    all_rows = []

    for ticker in ALL_TICKERS:
        asset = ASSET_MAP[ticker]
        last_date = existing.get(ticker)

        if last_date:
            start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"  {ticker:6s} ({asset}) — updating from {start}...", end=" ")
        else:
            start = "2024-01-01"
            print(f"  {ticker:6s} ({asset}) — full backfill from {start}...", end=" ")

        try:
            etf = yf.Ticker(ticker)

            # Get historical data
            hist = etf.history(start=start, end=now.strftime("%Y-%m-%d"), interval="1d")
            if hist.empty:
                print("no data")
                continue

            # Get shares outstanding (current)
            info = etf.info
            shares = info.get("sharesOutstanding") or info.get("totalAssets")

            # For ETFs, totalAssets is the AUM directly
            total_assets = info.get("totalAssets")

            for date, row in hist.iterrows():
                close = float(row["Close"]) if pd.notna(row["Close"]) else None
                if close is None or close <= 0:
                    continue

                # AUM estimate: if we have shares outstanding, use close * shares
                # Otherwise use totalAssets as the most recent snapshot
                aum = None
                if shares and shares > 0:
                    aum = close * shares
                elif total_assets and total_assets > 0:
                    aum = total_assets

                ts = date.strftime("%Y-%m-%d")
                all_rows.append({
                    "timestamp": ts,
                    "ticker": ticker,
                    "asset": asset,
                    "aum_usd": round(aum, 2) if aum else None,
                    "close_price": round(close, 4),
                    "shares_out": shares,
                    "source": "yfinance",
                    "ingested_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })

            print(f"{len(hist)} days")

        except Exception as e:
            print(f"ERROR: {e}")

    if all_rows:
        df = pd.DataFrame(all_rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.drop_duplicates(subset=["timestamp", "ticker"])

        print(f"\nLoading {len(df)} records...")

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("CREATE TEMP TABLE _tmp (LIKE etf_aum_daily INCLUDING DEFAULTS) ON COMMIT DROP")
            buf = StringIO()
            df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
            buf.seek(0)
            cols = list(df.columns)
            cur.copy_from(buf, "_tmp", columns=cols, null="\\N")
            col_str = ", ".join(cols)
            update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in ["aum_usd", "close_price", "shares_out", "source", "ingested_at"])
            cur.execute(f"INSERT INTO etf_aum_daily ({col_str}) SELECT {col_str} FROM _tmp ON CONFLICT (timestamp, ticker) DO UPDATE SET {update_str}")
            n = cur.rowcount
            conn.commit()
            print(f"+{n} rows upserted")
        except Exception as e:
            conn.rollback()
            print(f"Error: {e}")
        finally:
            conn.close()
    else:
        print("\nNo data fetched")

    print(f"\n{'━' * 60}")
    print(f"  DONE")
    print(f"{'━' * 60}")


if __name__ == "__main__":
    main()
