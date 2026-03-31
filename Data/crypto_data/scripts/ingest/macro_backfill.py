#!/usr/bin/env python3
"""
macro_backfill.py
-----------------
Pulls max available history of macro assets via yfinance.

Covers:
  - Daily OHLCV → max history (years)
  - Hourly OHLCV → last 60 days (yfinance limit for intraday)

Assets:
  - Equity indices: SPY, QQQ, IWM, DIA
  - Volatility: VIX
  - Bonds/Rates: TLT, IEF, SHY, ^TNX (10Y), ^IRX (2Y), ^TYX (30Y)
  - Commodities: GLD, SLV, BNO, USO, NG=F
  - FX / Dollar: DX-Y.NYB (DXY), EURUSD=X, JPYUSD=X
  - Bitcoin ETFs: IBIT, FBTC, ARKB, BITB
  - Ethereum ETFs: ETHA, ETHW
  - Crypto-adjacent equities: MSTR, COIN, MARA, RIOT

Output:
  macro/daily/macro_daily.csv
  macro/hourly/macro_hourly.csv

Schema → timestamp, ticker, name, asset_class, open, high, low, close, volume, source, ingested_at
"""

import sys
import time
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.utils.storage import write

# ── Asset universe ────────────────────────────────────────────────────────────
ASSETS = {
    # Equity indices
    "SPY":       ("S&P 500 ETF",              "equity_index"),
    "QQQ":       ("Nasdaq 100 ETF",           "equity_index"),
    "IWM":       ("Russell 2000 ETF",         "equity_index"),
    "DIA":       ("Dow Jones ETF",            "equity_index"),

    # Volatility
    "^VIX":      ("CBOE Volatility Index",    "volatility"),

    # Bonds & Rates
    "TLT":       ("US Treasury 20Y+ ETF",     "bonds"),
    "IEF":       ("US Treasury 7-10Y ETF",    "bonds"),
    "SHY":       ("US Treasury 1-3Y ETF",     "bonds"),
    "^TNX":      ("10Y Treasury Yield",       "rates"),
    "^IRX":      ("2Y Treasury Yield",        "rates"),
    "^TYX":      ("30Y Treasury Yield",       "rates"),

    # Commodities
    "GLD":       ("Gold ETF",                 "commodities"),
    "SLV":       ("Silver ETF",               "commodities"),
    "BNO":       ("Brent Oil ETF",            "commodities"),
    "USO":       ("WTI Oil ETF",              "commodities"),
    "NG=F":      ("Natural Gas Futures",      "commodities"),

    # FX / Dollar
    "DX-Y.NYB":  ("US Dollar Index",          "fx"),
    "EURUSD=X":  ("EUR/USD",                  "fx"),
    "JPYUSD=X":  ("JPY/USD",                  "fx"),

    # Bitcoin ETFs
    "IBIT":      ("iShares Bitcoin ETF",      "crypto_etf"),
    "FBTC":      ("Fidelity Bitcoin ETF",     "crypto_etf"),
    "ARKB":      ("ARK Bitcoin ETF",          "crypto_etf"),
    "BITB":      ("Bitwise Bitcoin ETF",      "crypto_etf"),

    # Ethereum ETFs
    "ETHA":      ("iShares Ethereum ETF",     "crypto_etf"),
    "ETHW":      ("Bitwise Ethereum ETF",     "crypto_etf"),

    # Crypto-adjacent equities
    "MSTR":      ("MicroStrategy",            "crypto_equity"),
    "COIN":      ("Coinbase",                 "crypto_equity"),
    "MARA":      ("Marathon Digital",         "crypto_equity"),
    "RIOT":      ("Riot Platforms",           "crypto_equity"),
}

SOURCE = "yfinance"
SLEEP  = 0.5   # seconds between tickers to be polite


# ── Fetch helpers ─────────────────────────────────────────────────────────────
def fetch_daily(ticker: str) -> pd.DataFrame:
    """Pull max daily history for a ticker."""
    try:
        df = yf.download(
            ticker,
            period="max",
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            return pd.DataFrame()

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"date": "timestamp", "adj close": "close"})

        # Ensure timestamp is UTC ISO string
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize("UTC", ambiguous="infer").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        cols = [c for c in ["timestamp", "open", "high", "low", "close", "volume"] if c in df.columns]
        return df[cols].dropna(subset=["close"])

    except Exception as e:
        print(f"  [error] daily fetch failed: {e}")
        return pd.DataFrame()


def fetch_hourly(ticker: str) -> pd.DataFrame:
    """Pull last 60 days of hourly data for a ticker."""
    try:
        df = yf.download(
            ticker,
            period="60d",
            interval="1h",
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            return pd.DataFrame()

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"datetime": "timestamp", "adj close": "close"})

        if "timestamp" not in df.columns and "date" in df.columns:
            df = df.rename(columns={"date": "timestamp"})

        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Drop current incomplete hour
        now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        df = df[pd.to_datetime(df["timestamp"], utc=True) < pd.Timestamp(now_utc)]

        cols = [c for c in ["timestamp", "open", "high", "low", "close", "volume"] if c in df.columns]
        return df[cols].dropna(subset=["close"])

    except Exception as e:
        print(f"  [error] hourly fetch failed: {e}")
        return pd.DataFrame()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    tickers = list(ASSETS.keys())
    print(f"\n[macro_backfill] {len(tickers)} assets\n")

    failed_daily  = []
    failed_hourly = []

    for i, ticker in enumerate(tickers, 1):
        name, asset_class = ASSETS[ticker]
        print(f"[{i}/{len(tickers)}] {ticker} — {name}")

        # ── Daily ─────────────────────────────────────────────────────────────
        df_d = fetch_daily(ticker)
        if df_d.empty:
            print(f"  daily: no data")
            failed_daily.append(ticker)
        else:
            df_d["ticker"]      = ticker
            df_d["name"]        = name
            df_d["asset_class"] = asset_class
            cols = ["timestamp", "ticker", "name", "asset_class"] + \
                   [c for c in ["open", "high", "low", "close", "volume"] if c in df_d.columns]
            write("macro_daily", df_d[cols], source=SOURCE)
            print(f"  daily: {len(df_d)} rows")

        time.sleep(SLEEP)

        # ── Hourly ────────────────────────────────────────────────────────────
        df_h = fetch_hourly(ticker)
        if df_h.empty:
            print(f"  hourly: no data")
            failed_hourly.append(ticker)
        else:
            df_h["ticker"]      = ticker
            df_h["name"]        = name
            df_h["asset_class"] = asset_class
            cols = ["timestamp", "ticker", "name", "asset_class"] + \
                   [c for c in ["open", "high", "low", "close", "volume"] if c in df_h.columns]
            write("macro_hourly", df_h[cols], source=SOURCE)
            print(f"  hourly: {len(df_h)} rows")

        time.sleep(SLEEP)

    print(f"\n[done] macro backfill complete")
    if failed_daily:
        print(f"[warn] daily failed: {failed_daily}")
    if failed_hourly:
        print(f"[warn] hourly failed: {failed_hourly}")


if __name__ == "__main__":
    main()
