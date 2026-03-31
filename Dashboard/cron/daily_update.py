#!/usr/bin/env python3
"""
daily_update.py
───────────────
Incremental daily updater for the Wintermute Dashboard.
Checks each table for the latest timestamp, fetches only missing data,
and upserts directly into Postgres.

Designed to run as a Railway cron service (e.g. 0 6 * * * UTC).
Safe to re-run — deduplicates on primary keys via ON CONFLICT DO NOTHING.

Environment variables required:
    DATABASE_URL          — Postgres connection string
    COINGECKO_API_KEY     — CoinGecko Pro API key

Usage:
    python daily_update.py              # run all updaters
    python daily_update.py prices       # run only crypto prices
    python daily_update.py derivatives  # run only derivatives
    python daily_update.py macro        # run only macro
    python daily_update.py volume       # run only volume
    python daily_update.py marketcap    # run only market cap
"""

import os
import sys
import time
import json
import requests
import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf
from datetime import datetime, timezone, timedelta
from io import StringIO

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DATABASE_URL    = os.environ["DATABASE_URL"]
CG_API_KEY      = os.environ["COINGECKO_API_KEY"]
CG_BASE         = "https://pro-api.coingecko.com/api/v3"

SLEEP_CG        = 0.25      # seconds between CoinGecko calls
SLEEP_DERIV     = 0.30      # seconds between exchange calls
SLEEP_MACRO     = 0.50      # seconds between yfinance calls
MAX_RETRIES     = 3
RETRY_BACKOFF   = 10
TIMEOUT         = 45

BINANCE_BASE    = "https://fapi.binance.com"
BYBIT_BASE      = "https://api.bybit.com"

# Macro asset universe (mirrors macro_backfill.py)
MACRO_ASSETS = {
    "SPY":       ("S&P 500 ETF",              "equity_index"),
    "QQQ":       ("Nasdaq 100 ETF",           "equity_index"),
    "IWM":       ("Russell 2000 ETF",         "equity_index"),
    "DIA":       ("Dow Jones ETF",            "equity_index"),
    "^VIX":      ("CBOE Volatility Index",    "volatility"),
    "TLT":       ("US Treasury 20Y+ ETF",     "bonds"),
    "IEF":       ("US Treasury 7-10Y ETF",    "bonds"),
    "SHY":       ("US Treasury 1-3Y ETF",     "bonds"),
    "^TNX":      ("10Y Treasury Yield",       "rates"),
    "^IRX":      ("2Y Treasury Yield",        "rates"),
    "^TYX":      ("30Y Treasury Yield",       "rates"),
    "GLD":       ("Gold ETF",                 "commodities"),
    "SLV":       ("Silver ETF",               "commodities"),
    "BNO":       ("Brent Oil ETF",            "commodities"),
    "USO":       ("WTI Oil ETF",              "commodities"),
    "NG=F":      ("Natural Gas Futures",      "commodities"),
    "DX-Y.NYB":  ("US Dollar Index",          "fx"),
    "EURUSD=X":  ("EUR/USD",                  "fx"),
    "JPYUSD=X":  ("JPY/USD",                  "fx"),
    "IBIT":      ("iShares Bitcoin ETF",      "crypto_etf"),
    "FBTC":      ("Fidelity Bitcoin ETF",     "crypto_etf"),
    "ARKB":      ("ARK Bitcoin ETF",          "crypto_etf"),
    "BITB":      ("Bitwise Bitcoin ETF",      "crypto_etf"),
    "ETHA":      ("iShares Ethereum ETF",     "crypto_etf"),
    "ETHW":      ("Bitwise Ethereum ETF",     "crypto_etf"),
    "MSTR":      ("MicroStrategy",            "crypto_equity"),
    "COIN":      ("Coinbase",                 "crypto_equity"),
    "MARA":      ("Marathon Digital",         "crypto_equity"),
    "RIOT":      ("Riot Platforms",           "crypto_equity"),
}


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def get_latest_timestamp(table: str, filter_col: str = None, filter_val: str = None) -> datetime | None:
    """Get the most recent timestamp in a table, optionally filtered."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        if filter_col and filter_val:
            cur.execute(
                f"SELECT MAX(timestamp) FROM {table} WHERE {filter_col} = %s",
                (filter_val,)
            )
        else:
            cur.execute(f"SELECT MAX(timestamp) FROM {table}")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def get_latest_per_asset(table: str, id_col: str = "coingecko_id") -> dict[str, datetime]:
    """Get the latest timestamp per asset in a table. Returns {id: datetime}."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {id_col}, MAX(timestamp) FROM {table} GROUP BY {id_col}")
        return {row[0]: row[1] for row in cur.fetchall() if row[1]}
    finally:
        conn.close()


def get_latest_per_asset_exchange(table: str) -> dict[tuple, datetime]:
    """Get latest timestamp per (coingecko_id, exchange). Returns {(id, exch): datetime}."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT coingecko_id, exchange, MAX(timestamp) FROM {table} GROUP BY coingecko_id, exchange")
        return {(row[0], row[1]): row[2] for row in cur.fetchall() if row[2]}
    finally:
        conn.close()


def bulk_upsert(table: str, df: pd.DataFrame, conflict_cols: list[str]):
    """
    Bulk insert rows into Postgres using COPY + temp table + INSERT ON CONFLICT.
    This is fast and handles dedup cleanly.
    """
    if df.empty:
        return 0

    conn = get_conn()
    try:
        cur = conn.cursor()
        cols = list(df.columns)
        col_str = ", ".join(cols)
        conflict_str = ", ".join(conflict_cols)

        # Create temp table
        cur.execute(f"CREATE TEMP TABLE _tmp (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")

        # COPY data into temp table
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "_tmp", columns=cols, null="\\N")

        # Upsert from temp → real table
        cur.execute(f"""
            INSERT INTO {table} ({col_str})
            SELECT {col_str} FROM _tmp
            ON CONFLICT ({conflict_str}) DO NOTHING
        """)

        inserted = cur.rowcount
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_registry_from_db() -> pd.DataFrame:
    """Load the asset registry from Postgres."""
    conn = get_conn()
    try:
        return pd.read_sql("SELECT symbol, coingecko_id FROM asset_registry WHERE coingecko_id IS NOT NULL", conn)
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

SESSION = requests.Session()


def cg_get(url: str, params: dict = {}) -> dict:
    """CoinGecko Pro API request with retries."""
    headers = {"Accept": "application/json", "x-cg-pro-api-key": CG_API_KEY}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = RETRY_BACKOFF * attempt
                print(f"    [429] sleeping {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                raise


def exch_get(url: str, params: dict = {}) -> dict | list:
    """Exchange API request with retries (Binance/Bybit)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                raise


# ══════════════════════════════════════════════════════════════════════════════
# 1. CRYPTO PRICES (price_daily)
# ══════════════════════════════════════════════════════════════════════════════

def update_prices():
    """Fetch missing daily prices for all assets from CoinGecko."""
    print("\n" + "=" * 70)
    print("UPDATING: price_daily")
    print("=" * 70)

    reg = get_registry_from_db()
    latest = get_latest_per_asset("price_daily")
    today = datetime.now(timezone.utc).date()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    total = len(reg)
    inserted_total = 0
    skipped = 0
    failed = []

    for i, row in reg.iterrows():
        cg_id  = row["coingecko_id"]
        symbol = row["symbol"]
        last   = latest.get(cg_id)

        # Calculate days to fetch
        if last:
            last_date = last.date() if hasattr(last, "date") else last
            gap_days = (today - last_date).days
            if gap_days <= 1:
                skipped += 1
                continue
        else:
            gap_days = "max"

        idx = i + 1
        print(f"  [{idx}/{total}] {symbol} ({cg_id}) — {gap_days} days gap … ", end="", flush=True)

        try:
            params = {"vs_currency": "usd", "days": str(gap_days), "interval": "daily"}
            data = cg_get(f"{CG_BASE}/coins/{cg_id}/market_chart", params)
            prices = data.get("prices", [])

            if not prices:
                print("no data")
                time.sleep(SLEEP_CG)
                continue

            df = pd.DataFrame(prices, columns=["ts_ms", "price_usd"])
            df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.normalize()

            # Drop today's partial candle
            df = df[df["timestamp"].dt.date < today]
            df = df.drop_duplicates(subset=["timestamp"], keep="last")

            # Only keep rows newer than what we have
            if last:
                df = df[df["timestamp"] > pd.Timestamp(last, tz="UTC")]

            if df.empty:
                print("up to date")
                time.sleep(SLEEP_CG)
                continue

            df["coingecko_id"] = cg_id
            df["symbol"]       = symbol
            df["source"]       = "coingecko"
            df["ingested_at"]  = now_utc
            df = df[["timestamp", "coingecko_id", "symbol", "price_usd", "source", "ingested_at"]]

            n = bulk_upsert("price_daily", df, ["timestamp", "coingecko_id"])
            inserted_total += n
            print(f"+{n} rows")

        except Exception as e:
            print(f"error: {e}")
            failed.append(cg_id)

        time.sleep(SLEEP_CG)

    print(f"\n[price_daily] done — {inserted_total} new rows, {skipped} skipped, {len(failed)} failed")
    if failed:
        print(f"  failed: {failed[:10]}{'…' if len(failed) > 10 else ''}")


# ══════════════════════════════════════════════════════════════════════════════
# 2. MARKET CAP (marketcap_daily)
# ══════════════════════════════════════════════════════════════════════════════

def update_marketcap():
    """Fetch missing daily market caps. Uses the same CoinGecko market_chart endpoint."""
    print("\n" + "=" * 70)
    print("UPDATING: marketcap_daily")
    print("=" * 70)

    reg = get_registry_from_db()
    latest = get_latest_per_asset("marketcap_daily")
    today = datetime.now(timezone.utc).date()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    total = len(reg)
    inserted_total = 0
    skipped = 0
    failed = []

    for i, row in reg.iterrows():
        cg_id  = row["coingecko_id"]
        symbol = row["symbol"]
        last   = latest.get(cg_id)

        if last:
            gap_days = (today - last.date()).days if hasattr(last, "date") else (today - last).days
            if gap_days <= 1:
                skipped += 1
                continue
        else:
            gap_days = "max"

        idx = i + 1
        print(f"  [{idx}/{total}] {symbol} — {gap_days}d gap … ", end="", flush=True)

        try:
            params = {"vs_currency": "usd", "days": str(gap_days), "interval": "daily"}
            data = cg_get(f"{CG_BASE}/coins/{cg_id}/market_chart", params)
            mcaps = data.get("market_caps", [])

            if not mcaps:
                print("no data")
                time.sleep(SLEEP_CG)
                continue

            df = pd.DataFrame(mcaps, columns=["ts_ms", "market_cap_usd"])
            df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.normalize()
            df = df[df["timestamp"].dt.date < today]
            df = df.drop_duplicates(subset=["timestamp"], keep="last")

            if last:
                df = df[df["timestamp"] > pd.Timestamp(last, tz="UTC")]

            if df.empty:
                print("up to date")
                time.sleep(SLEEP_CG)
                continue

            df["coingecko_id"] = cg_id
            df["symbol"]       = symbol
            df["source"]       = "coingecko"
            df["ingested_at"]  = now_utc
            df = df[["timestamp", "coingecko_id", "symbol", "market_cap_usd", "source", "ingested_at"]]

            n = bulk_upsert("marketcap_daily", df, ["timestamp", "coingecko_id"])
            inserted_total += n
            print(f"+{n}")

        except Exception as e:
            print(f"error: {e}")
            failed.append(cg_id)

        time.sleep(SLEEP_CG)

    print(f"\n[marketcap_daily] done — {inserted_total} new, {skipped} skipped, {len(failed)} failed")


# ══════════════════════════════════════════════════════════════════════════════
# 3. VOLUME (volume_daily)
# ══════════════════════════════════════════════════════════════════════════════

def update_volume():
    """Fetch missing daily volumes from CoinGecko."""
    print("\n" + "=" * 70)
    print("UPDATING: volume_daily")
    print("=" * 70)

    reg = get_registry_from_db()
    latest = get_latest_per_asset("volume_daily")
    today = datetime.now(timezone.utc).date()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    total = len(reg)
    inserted_total = 0
    skipped = 0
    failed = []

    for i, row in reg.iterrows():
        cg_id  = row["coingecko_id"]
        symbol = row["symbol"]
        last   = latest.get(cg_id)

        if last:
            gap_days = (today - last.date()).days if hasattr(last, "date") else (today - last).days
            if gap_days <= 1:
                skipped += 1
                continue
        else:
            gap_days = "max"

        idx = i + 1
        print(f"  [{idx}/{total}] {symbol} — {gap_days}d gap … ", end="", flush=True)

        try:
            params = {"vs_currency": "usd", "days": str(gap_days), "interval": "daily"}
            data = cg_get(f"{CG_BASE}/coins/{cg_id}/market_chart", params)
            vols = data.get("total_volumes", [])

            if not vols:
                print("no data")
                time.sleep(SLEEP_CG)
                continue

            df = pd.DataFrame(vols, columns=["ts_ms", "volume_usd"])
            df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.normalize()
            df = df[df["timestamp"].dt.date < today]
            df = df.drop_duplicates(subset=["timestamp"], keep="last")

            if last:
                df = df[df["timestamp"] > pd.Timestamp(last, tz="UTC")]

            if df.empty:
                print("up to date")
                time.sleep(SLEEP_CG)
                continue

            df["coingecko_id"] = cg_id
            df["symbol"]       = symbol
            df["source"]       = "coingecko"
            df["ingested_at"]  = now_utc
            df = df[["timestamp", "coingecko_id", "symbol", "volume_usd", "source", "ingested_at"]]

            n = bulk_upsert("volume_daily", df, ["timestamp", "coingecko_id"])
            inserted_total += n
            print(f"+{n}")

        except Exception as e:
            print(f"error: {e}")
            failed.append(cg_id)

        time.sleep(SLEEP_CG)

    print(f"\n[volume_daily] done — {inserted_total} new, {skipped} skipped, {len(failed)} failed")


# ══════════════════════════════════════════════════════════════════════════════
# 4. CG COMBINED (price + mcap + volume in one API call)
#    This is smarter: /coins/{id}/market_chart returns all three at once,
#    so we avoid 3x the API calls by updating all three tables per asset.
# ══════════════════════════════════════════════════════════════════════════════

def update_coingecko_combined():
    """
    Smart updater: fetches price, market_cap, and volume in a single API call
    per asset (CoinGecko market_chart returns all three). Updates all three
    tables from one request, cutting API usage by ~3x.
    """
    print("\n" + "=" * 70)
    print("UPDATING: price_daily + marketcap_daily + volume_daily (combined)")
    print("=" * 70)

    reg = get_registry_from_db()
    latest_price  = get_latest_per_asset("price_daily")
    latest_mcap   = get_latest_per_asset("marketcap_daily")
    latest_vol    = get_latest_per_asset("volume_daily")
    today = datetime.now(timezone.utc).date()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    total = len(reg)
    stats = {"price_daily": 0, "marketcap_daily": 0, "volume_daily": 0}
    skipped = 0
    failed = []

    for i, row in reg.iterrows():
        cg_id  = row["coingecko_id"]
        symbol = row["symbol"]

        # Find the oldest "last" across all three tables to determine gap
        lasts = [latest_price.get(cg_id), latest_mcap.get(cg_id), latest_vol.get(cg_id)]
        lasts_valid = [l for l in lasts if l is not None]

        if lasts_valid:
            oldest_last = min(lasts_valid)
            last_date = oldest_last.date() if hasattr(oldest_last, "date") else oldest_last
            gap_days = (today - last_date).days
            if gap_days <= 1:
                skipped += 1
                continue
        else:
            gap_days = 90  # new asset, fetch last 90 days (not max — that's for backfill)

        idx = i + 1
        print(f"  [{idx}/{total}] {symbol} — {gap_days}d gap … ", end="", flush=True)

        try:
            params = {"vs_currency": "usd", "days": str(gap_days), "interval": "daily"}
            data = cg_get(f"{CG_BASE}/coins/{cg_id}/market_chart", params)

            for key, table, val_col, last_ts in [
                ("prices",        "price_daily",     "price_usd",      latest_price.get(cg_id)),
                ("market_caps",   "marketcap_daily",  "market_cap_usd", latest_mcap.get(cg_id)),
                ("total_volumes", "volume_daily",     "volume_usd",     latest_vol.get(cg_id)),
            ]:
                raw = data.get(key, [])
                if not raw:
                    continue

                df = pd.DataFrame(raw, columns=["ts_ms", val_col])
                df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.normalize()
                df = df[df["timestamp"].dt.date < today]
                df = df.drop_duplicates(subset=["timestamp"], keep="last")

                if last_ts:
                    df = df[df["timestamp"] > pd.Timestamp(last_ts, tz="UTC")]

                if df.empty:
                    continue

                df["coingecko_id"] = cg_id
                df["symbol"]       = symbol
                df["source"]       = "coingecko"
                df["ingested_at"]  = now_utc
                df = df[["timestamp", "coingecko_id", "symbol", val_col, "source", "ingested_at"]]

                n = bulk_upsert(table, df, ["timestamp", "coingecko_id"])
                stats[table] += n

            print("ok")

        except Exception as e:
            print(f"error: {e}")
            failed.append(cg_id)

        time.sleep(SLEEP_CG)

    print(f"\n[combined CG] done — price: +{stats['price_daily']}, mcap: +{stats['marketcap_daily']}, vol: +{stats['volume_daily']}")
    print(f"  {skipped} skipped, {len(failed)} failed")
    if failed:
        print(f"  failed: {failed[:10]}{'…' if len(failed) > 10 else ''}")


# ══════════════════════════════════════════════════════════════════════════════
# 5. DERIVATIVES (funding_8h, open_interest_daily/hourly, long_short_ratio)
# ══════════════════════════════════════════════════════════════════════════════

def discover_exchange_symbols(reg_df: pd.DataFrame) -> tuple[dict, dict]:
    """Discover which registry assets have perp contracts on Binance/Bybit."""
    sym_to_cg = {row["symbol"].upper(): row["coingecko_id"] for _, row in reg_df.iterrows()}

    # Binance
    data = exch_get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    binance_bases = {s["baseAsset"].upper() for s in data["symbols"] if s["quoteAsset"] == "USDT"}
    binance_map = {sym_to_cg[b]: f"{b}USDT" for b in binance_bases if b in sym_to_cg}
    print(f"  [Binance] {len(binance_map)} assets with USDT perps")

    # Bybit
    data = exch_get(f"{BYBIT_BASE}/v5/market/instruments-info", {"category": "linear", "limit": 1000})
    bybit_bases = {s["baseCoin"].upper() for s in data["result"]["list"]
                   if s["quoteCoin"] == "USDT" and s["status"] == "Trading"}
    bybit_map = {sym_to_cg[b]: f"{b}USDT" for b in bybit_bases if b in sym_to_cg}
    print(f"  [Bybit]   {len(bybit_map)} assets with USDT perps")

    return binance_map, bybit_map


def _binance_funding_since(symbol: str, since_ms: int) -> pd.DataFrame:
    """Fetch Binance funding rates from a given timestamp forward."""
    rows = []
    start = since_ms
    while True:
        params = {"symbol": symbol, "limit": 1000, "startTime": start}
        data = exch_get(f"{BINANCE_BASE}/fapi/v1/fundingRate", params)
        if not data:
            break
        rows.extend(data)
        if len(data) < 1000:
            break
        start = data[-1]["fundingTime"] + 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["funding_rate"] = df["fundingRate"].astype(float)
    return df[["timestamp", "funding_rate"]].drop_duplicates("timestamp").sort_values("timestamp")


def _bybit_funding_since(symbol: str, since_ms: int) -> pd.DataFrame:
    """Fetch Bybit funding rates from a given timestamp forward."""
    rows, cursor = [], None
    while True:
        params = {"category": "linear", "symbol": symbol, "limit": 200, "startTime": since_ms}
        if cursor:
            params["cursor"] = cursor
        data = exch_get(f"{BYBIT_BASE}/v5/market/funding/history", params)
        batch = data.get("result", {}).get("list", [])
        if not batch:
            break
        rows.extend(batch)
        cursor = data.get("result", {}).get("nextPageCursor")
        if not cursor:
            break
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["fundingRateTimestamp"].astype(float), unit="ms", utc=True)
    df["funding_rate"] = df["fundingRate"].astype(float)
    return df[["timestamp", "funding_rate"]].drop_duplicates("timestamp").sort_values("timestamp")


def _binance_oi_since(symbol: str, period: str, since_ms: int) -> pd.DataFrame:
    """Fetch Binance OI history from a given timestamp forward."""
    rows = []
    start = since_ms
    while True:
        params = {"symbol": symbol, "period": period, "limit": 500, "startTime": start}
        data = exch_get(f"{BINANCE_BASE}/futures/data/openInterestHist", params)
        if not data:
            break
        rows.extend(data)
        if len(data) < 500:
            break
        start = data[-1]["timestamp"] + 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["oi_usd"]       = df["sumOpenInterestValue"].astype(float)
    df["oi_contracts"] = df["sumOpenInterest"].astype(float)
    return df[["timestamp", "oi_usd", "oi_contracts"]].drop_duplicates("timestamp").sort_values("timestamp")


def _bybit_oi_since(symbol: str, interval: str, since_ms: int) -> pd.DataFrame:
    """Fetch Bybit OI history from a given timestamp forward."""
    rows, cursor = [], None
    while True:
        params = {"category": "linear", "symbol": symbol, "intervalTime": interval, "limit": 200, "startTime": since_ms}
        if cursor:
            params["cursor"] = cursor
        data = exch_get(f"{BYBIT_BASE}/v5/market/open-interest", params)
        batch = data.get("result", {}).get("list", [])
        if not batch:
            break
        rows.extend(batch)
        cursor = data.get("result", {}).get("nextPageCursor")
        if not cursor:
            break
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["timestamp"].astype(float), unit="ms", utc=True)
    df["oi_contracts"] = df["openInterest"].astype(float)
    df["oi_usd"]       = None
    return df[["timestamp", "oi_usd", "oi_contracts"]].drop_duplicates("timestamp").sort_values("timestamp")


def _binance_ls_since(symbol: str, period: str, since_ms: int) -> pd.DataFrame:
    """Fetch Binance long/short ratio from a given timestamp forward."""
    rows = []
    start = since_ms
    while True:
        params = {"symbol": symbol, "period": period, "limit": 500, "startTime": start}
        data = exch_get(f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio", params)
        if not data:
            break
        rows.extend(data)
        if len(data) < 500:
            break
        start = data[-1]["timestamp"] + 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"]  = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["ls_ratio"]   = df["longShortRatio"].astype(float)
    df["buy_ratio"]  = df["longAccount"].astype(float)
    df["sell_ratio"] = df["shortAccount"].astype(float)
    df["period"]     = period
    return df[["timestamp", "ls_ratio", "period", "buy_ratio", "sell_ratio"]].drop_duplicates("timestamp").sort_values("timestamp")


def _bybit_ls_recent(symbol: str) -> pd.DataFrame:
    """Fetch Bybit long/short ratio (last 60 days only)."""
    data = exch_get(f"{BYBIT_BASE}/v5/market/account-ratio",
                    {"category": "linear", "symbol": symbol, "period": "1h", "limit": 500})
    result = data.get("result", {}).get("list", [])
    if not result:
        return pd.DataFrame()
    df = pd.DataFrame(result)
    df["timestamp"]  = pd.to_datetime(df["timestamp"].astype(float), unit="ms", utc=True)
    df["buy_ratio"]  = df["buyRatio"].astype(float)
    df["sell_ratio"] = df["sellRatio"].astype(float)
    df["ls_ratio"]   = df["buy_ratio"] / df["sell_ratio"].replace(0, float("nan"))
    df["period"]     = "1h"
    return df[["timestamp", "buy_ratio", "sell_ratio", "ls_ratio", "period"]].drop_duplicates("timestamp").sort_values("timestamp")


def update_derivatives():
    """Incremental update for all derivative tables."""
    print("\n" + "=" * 70)
    print("UPDATING: derivatives (funding_8h, OI daily/hourly, long_short_ratio)")
    print("=" * 70)

    reg = get_registry_from_db()
    binance_map, bybit_map = discover_exchange_symbols(reg)
    cg_to_sym = dict(zip(reg["coingecko_id"], reg["symbol"]))
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Get latest timestamps per (asset, exchange)
    latest_fr  = get_latest_per_asset_exchange("funding_8h")
    latest_oid = get_latest_per_asset_exchange("open_interest_daily")
    latest_oih = get_latest_per_asset_exchange("open_interest_hourly")
    latest_ls  = get_latest_per_asset_exchange("long_short_ratio")

    stats = {"funding_8h": 0, "oi_daily": 0, "oi_hourly": 0, "ls_ratio": 0}

    for exchange, mapping in [("binance", binance_map), ("bybit", bybit_map)]:
        total = len(mapping)
        failed = []

        for i, (cg_id, exch_sym) in enumerate(mapping.items(), 1):
            symbol = cg_to_sym.get(cg_id, cg_id)
            print(f"  [{exchange}] [{i}/{total}] {symbol} … ", end="", flush=True)

            try:
                # ── Funding ───────────────────────────────────────────────
                last_fr = latest_fr.get((cg_id, exchange))
                since_ms = int(last_fr.timestamp() * 1000) + 1 if last_fr else 0

                if exchange == "binance":
                    fr = _binance_funding_since(exch_sym, since_ms)
                else:
                    fr = _bybit_funding_since(exch_sym, since_ms)

                if not fr.empty:
                    fr["coingecko_id"] = cg_id
                    fr["symbol"]       = symbol
                    fr["exchange"]     = exchange
                    fr["source"]       = exchange
                    fr["ingested_at"]  = now_utc
                    n = bulk_upsert("funding_8h", fr[["timestamp", "coingecko_id", "symbol", "exchange", "funding_rate", "source", "ingested_at"]],
                                    ["timestamp", "coingecko_id", "exchange"])
                    stats["funding_8h"] += n
                time.sleep(SLEEP_DERIV)

                # ── OI daily ──────────────────────────────────────────────
                last_oid = latest_oid.get((cg_id, exchange))
                since_ms = int(last_oid.timestamp() * 1000) + 1 if last_oid else 0

                if exchange == "binance":
                    oi = _binance_oi_since(exch_sym, "1d", since_ms)
                else:
                    oi = _bybit_oi_since(exch_sym, "1d", since_ms)

                if not oi.empty:
                    oi["coingecko_id"] = cg_id
                    oi["symbol"]       = symbol
                    oi["exchange"]     = exchange
                    oi["source"]       = exchange
                    oi["ingested_at"]  = now_utc
                    n = bulk_upsert("open_interest_daily", oi[["timestamp", "coingecko_id", "symbol", "exchange", "oi_usd", "oi_contracts", "source", "ingested_at"]],
                                    ["timestamp", "coingecko_id", "exchange"])
                    stats["oi_daily"] += n
                time.sleep(SLEEP_DERIV)

                # ── OI hourly ─────────────────────────────────────────────
                last_oih = latest_oih.get((cg_id, exchange))
                since_ms = int(last_oih.timestamp() * 1000) + 1 if last_oih else 0

                if exchange == "binance":
                    oih = _binance_oi_since(exch_sym, "1h", since_ms)
                else:
                    oih = _bybit_oi_since(exch_sym, "1h", since_ms)

                if not oih.empty:
                    oih["coingecko_id"] = cg_id
                    oih["symbol"]       = symbol
                    oih["exchange"]     = exchange
                    oih["source"]       = exchange
                    oih["ingested_at"]  = now_utc
                    n = bulk_upsert("open_interest_hourly", oih[["timestamp", "coingecko_id", "symbol", "exchange", "oi_usd", "oi_contracts", "source", "ingested_at"]],
                                    ["timestamp", "coingecko_id", "exchange"])
                    stats["oi_hourly"] += n
                time.sleep(SLEEP_DERIV)

                # ── Long/short ratio ──────────────────────────────────────
                last_ls_ts = latest_ls.get((cg_id, exchange))
                since_ms = int(last_ls_ts.timestamp() * 1000) + 1 if last_ls_ts else 0

                if exchange == "binance":
                    ls = _binance_ls_since(exch_sym, "1d", since_ms)
                else:
                    ls = _bybit_ls_recent(exch_sym)
                    # Filter to only new rows
                    if not ls.empty and last_ls_ts:
                        ls = ls[ls["timestamp"] > pd.Timestamp(last_ls_ts, tz="UTC")]

                if not ls.empty:
                    ls["coingecko_id"] = cg_id
                    ls["symbol"]       = symbol
                    ls["exchange"]     = exchange
                    ls["source"]       = exchange
                    ls["ingested_at"]  = now_utc
                    n = bulk_upsert("long_short_ratio",
                                    ls[["timestamp", "coingecko_id", "symbol", "exchange", "ls_ratio", "period", "buy_ratio", "sell_ratio", "source", "ingested_at"]],
                                    ["timestamp", "coingecko_id", "exchange", "period"])
                    stats["ls_ratio"] += n
                time.sleep(SLEEP_DERIV)

                print("ok")

            except Exception as e:
                print(f"error: {e}")
                failed.append(exch_sym)

        if failed:
            print(f"  [{exchange}] failed: {failed[:10]}")

    print(f"\n[derivatives] done — funding: +{stats['funding_8h']}, OI daily: +{stats['oi_daily']}, "
          f"OI hourly: +{stats['oi_hourly']}, L/S: +{stats['ls_ratio']}")


# ══════════════════════════════════════════════════════════════════════════════
# 6. MACRO (macro_daily, macro_hourly)
# ══════════════════════════════════════════════════════════════════════════════

def update_macro():
    """Incremental update for macro assets via yfinance."""
    print("\n" + "=" * 70)
    print("UPDATING: macro_daily + macro_hourly")
    print("=" * 70)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    today = datetime.now(timezone.utc).date()

    # Get latest per ticker
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker, MAX(timestamp) FROM macro_daily GROUP BY ticker")
        latest_daily = {row[0]: row[1] for row in cur.fetchall() if row[1]}
        cur.execute("SELECT ticker, MAX(timestamp) FROM macro_hourly GROUP BY ticker")
        latest_hourly = {row[0]: row[1] for row in cur.fetchall() if row[1]}
    finally:
        conn.close()

    stats = {"daily": 0, "hourly": 0}
    failed = []

    for i, (ticker, (name, asset_class)) in enumerate(MACRO_ASSETS.items(), 1):
        print(f"  [{i}/{len(MACRO_ASSETS)}] {ticker} — {name}")

        # ── Daily ─────────────────────────────────────────────────────
        last_d = latest_daily.get(ticker)
        if last_d:
            last_date = last_d.date() if hasattr(last_d, "date") else last_d
            gap = (today - last_date).days
            if gap <= 1:
                print(f"    daily: up to date")
            else:
                start_str = (last_date + timedelta(days=1)).isoformat()
                try:
                    df = yf.download(ticker, start=start_str, interval="1d", auto_adjust=True, progress=False)
                    if not df.empty:
                        if isinstance(df.columns, pd.MultiIndex):
                            df.columns = df.columns.get_level_values(0)
                        df = df.reset_index()
                        df.columns = [c.lower() for c in df.columns]
                        df = df.rename(columns={"date": "timestamp", "adj close": "close"})
                        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC").dt.floor("D")
                        df = df[df["timestamp"].dt.date < today]
                        if not df.empty:
                            df["ticker"]      = ticker
                            df["name"]        = name
                            df["asset_class"] = asset_class
                            df["source"]      = "yfinance"
                            df["ingested_at"] = now_utc
                            cols = ["timestamp", "ticker", "name", "asset_class",
                                    "open", "high", "low", "close", "volume", "source", "ingested_at"]
                            cols = [c for c in cols if c in df.columns]
                            n = bulk_upsert("macro_daily", df[cols], ["timestamp", "ticker"])
                            stats["daily"] += n
                            print(f"    daily: +{n}")
                        else:
                            print(f"    daily: no new data")
                    else:
                        print(f"    daily: no data returned")
                except Exception as e:
                    print(f"    daily error: {e}")
                    failed.append(ticker)
        else:
            print(f"    daily: no existing data (run backfill first)")

        # ── Hourly ────────────────────────────────────────────────────
        # yfinance only gives 60 days of hourly, so always fetch the full window
        # and let ON CONFLICT handle dedup
        try:
            df = yf.download(ticker, period="60d", interval="1h", auto_adjust=True, progress=False)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.reset_index()
                df.columns = [c.lower() for c in df.columns]
                df = df.rename(columns={"datetime": "timestamp", "date": "timestamp", "adj close": "close"})
                df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("UTC")

                # Only keep rows newer than what we have
                last_h = latest_hourly.get(ticker)
                if last_h:
                    df = df[df["timestamp"] > pd.Timestamp(last_h, tz="UTC")]

                if not df.empty:
                    df["ticker"]      = ticker
                    df["name"]        = name
                    df["asset_class"] = asset_class
                    df["source"]      = "yfinance"
                    df["ingested_at"] = now_utc
                    cols = ["timestamp", "ticker", "name", "asset_class",
                            "open", "high", "low", "close", "volume", "source", "ingested_at"]
                    cols = [c for c in cols if c in df.columns]
                    n = bulk_upsert("macro_hourly", df[cols], ["timestamp", "ticker"])
                    stats["hourly"] += n
                    print(f"    hourly: +{n}")
                else:
                    print(f"    hourly: up to date")
            else:
                print(f"    hourly: no data")
        except Exception as e:
            print(f"    hourly error: {e}")

        time.sleep(SLEEP_MACRO)

    print(f"\n[macro] done — daily: +{stats['daily']}, hourly: +{stats['hourly']}")
    if failed:
        print(f"  failed: {failed}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    start = time.time()
    print(f"\n{'━' * 70}")
    print(f"  WINTERMUTE DASHBOARD — DAILY UPDATE")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'━' * 70}")

    target = sys.argv[1].lower() if len(sys.argv) > 1 else "all"

    if target == "all":
        update_coingecko_combined()   # price + mcap + volume in one pass
        update_derivatives()
        update_macro()
    elif target in ("prices", "price"):
        update_coingecko_combined()
    elif target in ("derivatives", "derivs"):
        update_derivatives()
    elif target in ("macro",):
        update_macro()
    elif target == "volume":
        update_volume()
    elif target == "marketcap":
        update_marketcap()
    else:
        print(f"Unknown target: {target}")
        print("Usage: python daily_update.py [all|prices|derivatives|macro|volume|marketcap]")
        sys.exit(1)

    elapsed = time.time() - start
    print(f"\n{'━' * 70}")
    print(f"  COMPLETE — {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"{'━' * 70}\n")


if __name__ == "__main__":
    main()
