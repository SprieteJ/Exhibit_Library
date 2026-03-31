#!/usr/bin/env python3
"""
price_hourly_backfill.py
------------------------
Pulls 90 days of hourly USD prices from CoinGecko Pro
for all assets in the registry (90 days is the CoinGecko max for hourly).

Run once to initialise. Safe to re-run — deduplicates on (timestamp, coingecko_id).

Output → price/hourly/price_hourly.csv
Schema  → timestamp, coingecko_id, symbol, price_usd, source, ingested_at
"""

import os
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
ENV_PATH        = "/home/jasperdemaere/Master_JDM/Exhibit_Library/env/env.txt"
CG_BASE         = "https://pro-api.coingecko.com/api/v3"
TIMEOUT         = 45
SLEEP_BETWEEN   = 0.20
MAX_RETRIES     = 3
RETRY_BACKOFF   = 10
SOURCE          = "coingecko"


# ── API key ───────────────────────────────────────────────────────────────────
def load_api_key(env_path: str) -> str:
    try:
        from dotenv import load_dotenv
        if os.path.exists(env_path):
            load_dotenv(env_path)
        key = os.getenv("COINGECKO_API_KEY")
        if key:
            return key
    except Exception:
        pass
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    if k.strip() == "COINGECKO_API_KEY":
                        return v.strip()
    raise ValueError(f"COINGECKO_API_KEY not found in {env_path}")


# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_price_hourly(
    session: requests.Session,
    cg_id: str,
    api_key: str,
) -> pd.DataFrame:
    """
    90 days of hourly data via:
    /coins/{id}/market_chart?days=90&interval=hourly
    CoinGecko automatically returns hourly granularity for days <= 90.
    """
    url     = f"{CG_BASE}/coins/{cg_id}/market_chart"
    params  = {"vs_currency": "usd", "days": "90", "interval": "hourly"}
    headers = {"Accept": "application/json", "x-cg-pro-api-key": api_key}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                wait = RETRY_BACKOFF * attempt
                print(f"  [429] sleeping {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            prices = r.json().get("prices", []) or []
            if not prices:
                return pd.DataFrame()

            df = pd.DataFrame(prices, columns=["ts_ms", "price_usd"])
            df["timestamp"] = (
                pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            # Drop current incomplete hour
            now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
            df = df[pd.to_datetime(df["timestamp"], utc=True) < pd.Timestamp(now_utc)]

            df = (
                df[["timestamp", "price_usd"]]
                .drop_duplicates(subset=["timestamp"], keep="last")
                .sort_values("timestamp")
            )
            return df

        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                print(f"  [warn] attempt {attempt}/{MAX_RETRIES} failed: {e} — retrying in {wait}s")
                time.sleep(wait)
            else:
                raise

    return pd.DataFrame()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    api_key   = load_api_key(ENV_PATH)
    reg       = Registry()
    id_to_sym = reg.coingecko_to_symbol()
    ids       = reg.coingecko_ids()

    print(f"\n[price_hourly_backfill] {len(ids)} assets | 90 days hourly\n")

    session = requests.Session()
    failed  = []

    for i, cg_id in enumerate(ids, 1):
        symbol = id_to_sym.get(cg_id, cg_id)
        print(f"[{i}/{len(ids)}] {symbol} ({cg_id}) … ", end="", flush=True)

        try:
            df = fetch_price_hourly(session, cg_id, api_key)
            if df.empty:
                print("no data")
                failed.append(cg_id)
                time.sleep(SLEEP_BETWEEN)
                continue

            df["coingecko_id"] = cg_id
            df["symbol"]       = symbol
            df = df[["timestamp", "coingecko_id", "symbol", "price_usd"]]

            write("price_hourly", df, source=SOURCE)
            print(f"{len(df)} rows")

        except Exception as e:
            print(f"error: {e}")
            failed.append(cg_id)

        time.sleep(SLEEP_BETWEEN)

    print(f"\n[done] hourly backfill complete")
    if failed:
        print(f"[warn] failed ({len(failed)}): {failed[:15]}{'…' if len(failed) > 15 else ''}")


if __name__ == "__main__":
    main()
