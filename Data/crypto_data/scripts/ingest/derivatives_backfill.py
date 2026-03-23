#!/usr/bin/env python3
"""
derivatives_backfill.py
-----------------------
Pulls full available history of derivatives data from Binance and Bybit
for all assets in the registry that have perpetual futures markets.

Covers:
  - Funding rates (8h)
  - Open interest (daily + hourly)
  - Liquidations
  - Long/short ratio

The script first discovers which registry assets have perp contracts on each
exchange, builds a symbol map (BTCUSDT → bitcoin), then pulls all data types.

Outputs:
  funding/8h/funding_8h.csv
  open_interest/daily/oi_daily.csv
  open_interest/hourly/oi_hourly.csv
  derivatives/liquidations/liquidations.csv
  derivatives/long_short/long_short_ratio.csv

Schema shared columns: timestamp, coingecko_id, symbol, exchange, ...
"""

import sys
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.utils.registry import Registry
from scripts.utils.storage import write

# ── Config ────────────────────────────────────────────────────────────────────
SLEEP           = 0.25   # seconds between requests
MAX_RETRIES     = 3
RETRY_BACKOFF   = 10
LIMIT           = 1000   # max rows per API call (both exchanges)

BINANCE_BASE    = "https://fapi.binance.com"
BYBIT_BASE      = "https://api.bybit.com"


# ── HTTP helper ───────────────────────────────────────────────────────────────
def get(session: requests.Session, url: str, params: dict = {}) -> dict | list:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = RETRY_BACKOFF * attempt
                print(f"  [429] sleeping {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                raise


# ══════════════════════════════════════════════════════════════════════════════
# SYMBOL DISCOVERY
# Maps registry coingecko_ids → exchange perp symbols (e.g. bitcoin → BTCUSDT)
# ══════════════════════════════════════════════════════════════════════════════

def discover_binance_symbols(session: requests.Session, reg: Registry) -> dict[str, str]:
    """Returns {coingecko_id: binance_symbol} for assets with USDT perps on Binance."""
    data      = get(session, f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    available = {s["baseAsset"].upper() for s in data["symbols"] if s["quoteAsset"] == "USDT"}
    sym_to_cg = {v.upper(): k for k, v in reg.coingecko_to_symbol().items()}  # SYMBOL → cg_id

    mapping = {}
    for base in available:
        cg_id = sym_to_cg.get(base)
        if cg_id:
            mapping[cg_id] = f"{base}USDT"

    print(f"[Binance] {len(mapping)} assets with USDT perps found in registry")
    return mapping


def discover_bybit_symbols(session: requests.Session, reg: Registry) -> dict[str, str]:
    """Returns {coingecko_id: bybit_symbol} for assets with USDT perps on Bybit."""
    data      = get(session, f"{BYBIT_BASE}/v5/market/instruments-info",
                    {"category": "linear", "limit": 1000})
    available = {s["baseCoin"].upper() for s in data["result"]["list"]
                 if s["quoteCoin"] == "USDT" and s["status"] == "Trading"}
    sym_to_cg = {v.upper(): k for k, v in reg.coingecko_to_symbol().items()}

    mapping = {}
    for base in available:
        cg_id = sym_to_cg.get(base)
        if cg_id:
            mapping[cg_id] = f"{base}USDT"

    print(f"[Bybit]   {len(mapping)} assets with USDT perps found in registry")
    return mapping


# ══════════════════════════════════════════════════════════════════════════════
# BINANCE FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def binance_funding_history(session, symbol: str) -> pd.DataFrame:
    """Full funding rate history. Paginates until exhausted."""
    rows, end_time = [], None
    while True:
        params = {"symbol": symbol, "limit": LIMIT}
        if end_time:
            params["endTime"] = end_time
        data = get(session, f"{BINANCE_BASE}/fapi/v1/fundingRate", params)
        if not data:
            break
        rows.extend(data)
        if len(data) < LIMIT:
            break
        end_time = data[0]["fundingTime"] - 1  # paginate backwards

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["fundingTime"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["funding_rate"] = df["fundingRate"].astype(float)
    return df[["timestamp", "funding_rate"]].drop_duplicates("timestamp").sort_values("timestamp")


def binance_open_interest_history(session, symbol: str, period: str = "1d") -> pd.DataFrame:
    """OI history. period: 5m,15m,30m,1h,2h,4h,6h,12h,1d"""
    rows, end_time = [], None
    while True:
        params = {"symbol": symbol, "period": period, "limit": LIMIT}
        if end_time:
            params["endTime"] = end_time
        data = get(session, f"{BINANCE_BASE}/futures/data/openInterestHist", params)
        if not data:
            break
        rows.extend(data)
        if len(data) < LIMIT:
            break
        end_time = data[0]["timestamp"] - 1

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"]       = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["oi_usd"]          = df["sumOpenInterestValue"].astype(float)
    df["oi_contracts"]    = df["sumOpenInterest"].astype(float)
    return df[["timestamp", "oi_usd", "oi_contracts"]].drop_duplicates("timestamp").sort_values("timestamp")


def binance_liquidations(session, symbol: str) -> pd.DataFrame:
    """Recent liquidation orders (Binance only provides ~30 days)."""
    data = get(session, f"{BINANCE_BASE}/fapi/v1/allForceOrders",
               {"symbol": symbol, "limit": LIMIT})
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["side"]      = df["side"].str.lower()
    df["size_usd"]  = df["origQty"].astype(float) * df["price"].astype(float)
    return df[["timestamp", "side", "size_usd"]].sort_values("timestamp")


def binance_long_short(session, symbol: str, period: str = "1d") -> pd.DataFrame:
    """Global long/short account ratio. period: 5m,15m,30m,1h,2h,4h,6h,12h,1d"""
    rows, end_time = [], None
    while True:
        params = {"symbol": symbol, "period": period, "limit": LIMIT}
        if end_time:
            params["endTime"] = end_time
        data = get(session, f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio", params)
        if not data:
            break
        rows.extend(data)
        if len(data) < LIMIT:
            break
        end_time = data[0]["timestamp"] - 1

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"]   = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["long_pct"]    = df["longAccount"].astype(float)
    df["short_pct"]   = df["shortAccount"].astype(float)
    df["ls_ratio"]    = df["longShortRatio"].astype(float)
    df["period"]      = period
    return df[["timestamp", "long_pct", "short_pct", "ls_ratio", "period"]].drop_duplicates("timestamp").sort_values("timestamp")


# ══════════════════════════════════════════════════════════════════════════════
# BYBIT FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def bybit_funding_history(session, symbol: str) -> pd.DataFrame:
    """Full funding rate history from Bybit."""
    rows, cursor = [], None
    while True:
        params = {"category": "linear", "symbol": symbol, "limit": LIMIT}
        if cursor:
            params["cursor"] = cursor
        data   = get(session, f"{BYBIT_BASE}/v5/market/funding/history", params)
        result = data.get("result", {})
        batch  = result.get("list", [])
        if not batch:
            break
        rows.extend(batch)
        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["fundingRateTimestamp"].astype(float), unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["funding_rate"] = df["fundingRate"].astype(float)
    return df[["timestamp", "funding_rate"]].drop_duplicates("timestamp").sort_values("timestamp")


def bybit_open_interest(session, symbol: str, interval: str = "1d") -> pd.DataFrame:
    """OI history from Bybit. interval: 5min,15min,30min,1h,4h,1d"""
    rows, cursor = [], None
    while True:
        params = {"category": "linear", "symbol": symbol,
                  "intervalTime": interval, "limit": LIMIT}
        if cursor:
            params["cursor"] = cursor
        data   = get(session, f"{BYBIT_BASE}/v5/market/open-interest", params)
        result = data.get("result", {})
        batch  = result.get("list", [])
        if not batch:
            break
        rows.extend(batch)
        cursor = result.get("nextPageCursor")
        if not cursor:
            break

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"]    = pd.to_datetime(df["timestamp"].astype(float), unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["oi_contracts"] = df["openInterest"].astype(float)
    df["oi_usd"]       = float("nan")  # Bybit OI history doesn't include USD value
    return df[["timestamp", "oi_usd", "oi_contracts"]].drop_duplicates("timestamp").sort_values("timestamp")


def bybit_long_short(session, symbol: str) -> pd.DataFrame:
    """Long/short ratio from Bybit (last 60 days, 1h granularity)."""
    data   = get(session, f"{BYBIT_BASE}/v5/market/account-ratio",
                 {"category": "linear", "symbol": symbol, "period": "1h", "limit": LIMIT})
    result = data.get("result", {}).get("list", [])
    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(result)
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(float), unit="ms", utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["buy_ratio"]  = df["buyRatio"].astype(float)
    df["sell_ratio"] = df["sellRatio"].astype(float)
    df["ls_ratio"]   = df["buy_ratio"] / df["sell_ratio"].replace(0, float("nan"))
    df["period"]     = "1h"
    return df[["timestamp", "buy_ratio", "sell_ratio", "ls_ratio", "period"]].drop_duplicates("timestamp").sort_values("timestamp")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_exchange(
    session: requests.Session,
    exchange: str,
    mapping: dict[str, str],
    id_to_sym: dict[str, str],
):
    total   = len(mapping)
    failed  = []

    for i, (cg_id, exch_symbol) in enumerate(mapping.items(), 1):
        symbol = id_to_sym.get(cg_id, cg_id)
        print(f"\n[{exchange}] [{i}/{total}] {symbol} ({exch_symbol})")

        try:
            # ── Funding rates ─────────────────────────────────────────────
            if exchange == "binance":
                fr = binance_funding_history(session, exch_symbol)
            else:
                fr = bybit_funding_history(session, exch_symbol)

            if not fr.empty:
                fr["coingecko_id"] = cg_id
                fr["symbol"]       = symbol
                fr["exchange"]     = exchange
                write("funding_8h", fr[["timestamp", "coingecko_id", "symbol", "exchange", "funding_rate"]], source=exchange)
            time.sleep(SLEEP)

            # ── Open interest daily ───────────────────────────────────────
            if exchange == "binance":
                oi_d = binance_open_interest_history(session, exch_symbol, "1d")
            else:
                oi_d = bybit_open_interest(session, exch_symbol, "1d")

            if not oi_d.empty:
                oi_d["coingecko_id"] = cg_id
                oi_d["symbol"]       = symbol
                oi_d["exchange"]     = exchange
                write("open_interest_daily", oi_d[["timestamp", "coingecko_id", "symbol", "exchange", "oi_usd", "oi_contracts"]], source=exchange)
            time.sleep(SLEEP)

            # ── Open interest hourly ──────────────────────────────────────
            if exchange == "binance":
                oi_h = binance_open_interest_history(session, exch_symbol, "1h")
            else:
                oi_h = bybit_open_interest(session, exch_symbol, "1h")

            if not oi_h.empty:
                oi_h["coingecko_id"] = cg_id
                oi_h["symbol"]       = symbol
                oi_h["exchange"]     = exchange
                write("open_interest_hourly", oi_h[["timestamp", "coingecko_id", "symbol", "exchange", "oi_usd", "oi_contracts"]], source=exchange)
            time.sleep(SLEEP)

            # ── Liquidations ──────────────────────────────────────────────
            if exchange == "binance":
                liq = binance_liquidations(session, exch_symbol)
                if not liq.empty:
                    liq["coingecko_id"] = cg_id
                    liq["symbol"]       = symbol
                    liq["exchange"]     = exchange
                    write("liquidations", liq[["timestamp", "coingecko_id", "symbol", "exchange", "side", "size_usd"]], source=exchange)
            time.sleep(SLEEP)

            # ── Long/short ratio ──────────────────────────────────────────
            if exchange == "binance":
                ls = binance_long_short(session, exch_symbol, "1d")
            else:
                ls = bybit_long_short(session, exch_symbol)

            if not ls.empty:
                ls["coingecko_id"] = cg_id
                ls["symbol"]       = symbol
                ls["exchange"]     = exchange
                cols = ["timestamp", "coingecko_id", "symbol", "exchange", "ls_ratio", "period"]
                if "long_pct" in ls.columns:
                    cols += ["long_pct", "short_pct"]
                if "buy_ratio" in ls.columns:
                    cols += ["buy_ratio", "sell_ratio"]
                write("long_short_ratio", ls[[c for c in cols if c in ls.columns]], source=exchange)
            time.sleep(SLEEP)

        except Exception as e:
            print(f"  [error] {symbol}: {e}")
            failed.append(exch_symbol)

    return failed


def main():
    reg       = Registry()
    id_to_sym = reg.coingecko_to_symbol()
    session   = requests.Session()

    print("\n[derivatives_backfill] Discovering symbols...\n")
    binance_map = discover_binance_symbols(session, reg)
    bybit_map   = discover_bybit_symbols(session, reg)

    print(f"\n[derivatives_backfill] Starting Binance ({len(binance_map)} assets)...")
    failed_binance = run_exchange(session, "binance", binance_map, id_to_sym)

    print(f"\n[derivatives_backfill] Starting Bybit ({len(bybit_map)} assets)...")
    failed_bybit = run_exchange(session, "bybit", bybit_map, id_to_sym)

    print(f"\n[done] derivatives backfill complete")
    if failed_binance:
        print(f"[warn] Binance failed ({len(failed_binance)}): {failed_binance[:10]}")
    if failed_bybit:
        print(f"[warn] Bybit failed ({len(failed_bybit)}): {failed_bybit[:10]}")


if __name__ == "__main__":
    main()
