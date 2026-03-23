#!/usr/bin/env python3
"""
marketcap_backfill.py
---------------------
Pulls FULL history of daily market caps from CoinGecko Pro
for all assets in the registry.

Run once to initialise. Safe to re-run — deduplicates on (timestamp, coingecko_id).

Output → /home/jasperdemaere/Master_JDM/Exhibit_Library/Data/crypto_data/marketcap/daily/marketcap_daily.csv
Schema  → timestamp, coingecko_id, symbol, market_cap_usd, source, ingested_at
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
def fetch_marketcap_max(
    session: requests.Session,
    cg_id: str,
    api_key: str,
) -> pd.DataFrame:
    """Full history via /coins/{id}/market_chart?days=max&interval=daily"""
    url     = f"{CG_BASE}/coins/{cg_id}/market_chart"
    params  = {"vs_currency": "usd", "days": "max", "interval": "daily"}
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
            caps = r.json().get("market_caps", []) or []
            if not caps:
                return pd.DataFrame()

            df = pd.DataFrame(caps, columns=["ts_ms", "market_cap_usd"])
            df["timestamp"] = (
                pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
                .dt.normalize()
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            # Drop today's partial candle
            today = datetime.now(timezone.utc).date()
            df = df[pd.to_datetime(df["timestamp"]).dt.date < today]
            df = (
                df[["timestamp", "market_cap_usd"]]
                .drop_duplicates(subset=["timestamp"], keep="last")
                .sort_values("timestamp")
            )
            df = df[df["market_cap_usd"] > 0]
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
    api_key     = load_api_key(ENV_PATH)
    reg         = Registry()
    id_to_sym   = reg.coingecko_to_symbol()
    ids         = reg.coingecko_ids()

    print(f"\n[marketcap_backfill] {len(ids)} assets to fetch\n")

    session = requests.Session()
    failed  = []

    for i, cg_id in enumerate(ids, 1):
        symbol = id_to_sym.get(cg_id, cg_id)
        print(f"[{i}/{len(ids)}] {symbol} ({cg_id}) … ", end="", flush=True)

        try:
            df = fetch_marketcap_max(session, cg_id, api_key)
            if df.empty:
                print("no data")
                failed.append(cg_id)
                time.sleep(SLEEP_BETWEEN)
                continue

            df["coingecko_id"] = cg_id
            df["symbol"]       = symbol
            df = df[["timestamp", "coingecko_id", "symbol", "market_cap_usd"]]

            write("marketcap_daily", df, source=SOURCE)
            print(f"{len(df)} rows")

        except Exception as e:
            print(f"error: {e}")
            failed.append(cg_id)

        time.sleep(SLEEP_BETWEEN)

    print(f"\n[done] backfill complete")
    if failed:
        print(f"[warn] failed ({len(failed)}): {failed[:15]}{'…' if len(failed) > 15 else ''}")


if __name__ == "__main__":
    main()
