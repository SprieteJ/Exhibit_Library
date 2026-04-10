#!/usr/bin/env python3
"""
options_daily_backfill.py
─────────────────────────
Pulls BTC + ETH options summary from Deribit public API.
Computes: total OI, put/call ratio, volume, max pain, ATM IV.
Stores one row per day per currency.

Run:
  DATABASE_URL="postgresql://..." python3 options_daily_backfill.py
"""

import os, requests, psycopg2, psycopg2.extras, math
from datetime import datetime, timezone
from collections import defaultdict

DATABASE_URL = os.environ["DATABASE_URL"]
DERIBIT_BASE = "https://www.deribit.com/api/v2/public"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def create_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS options_daily (
            timestamp        TIMESTAMPTZ NOT NULL,
            currency         TEXT NOT NULL,
            total_oi_contracts DOUBLE PRECISION,
            total_oi_usd     DOUBLE PRECISION,
            total_volume_usd DOUBLE PRECISION,
            put_oi           DOUBLE PRECISION,
            call_oi          DOUBLE PRECISION,
            pc_ratio_oi      DOUBLE PRECISION,
            put_volume       DOUBLE PRECISION,
            call_volume      DOUBLE PRECISION,
            pc_ratio_volume  DOUBLE PRECISION,
            max_pain_nearest DOUBLE PRECISION,
            avg_iv_atm       DOUBLE PRECISION,
            source           TEXT,
            ingested_at      TIMESTAMPTZ,
            UNIQUE (timestamp, currency)
        )
    """)
    conn.commit()
    conn.close()
    print("[OK] Table options_daily ready")


def fetch_options_summary(currency):
    """Fetch all option book summaries for a currency."""
    resp = requests.get(
        f"{DERIBIT_BASE}/get_book_summary_by_currency",
        params={"currency": currency, "kind": "option"},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def get_underlying_price(currency):
    """Get current underlying price."""
    resp = requests.get(
        f"{DERIBIT_BASE}/get_index_price",
        params={"index_name": f"{currency.lower()}_usd"},
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()["result"]["index_price"]


def parse_instrument(name):
    """Parse instrument name like BTC-26MAR27-60000-C into components."""
    parts = name.split("-")
    if len(parts) != 4:
        return None
    return {
        "currency": parts[0],
        "expiry": parts[1],
        "strike": float(parts[2]),
        "type": "put" if parts[3] == "P" else "call",
    }


def compute_max_pain(options, nearest_expiry):
    """Compute max pain for the nearest expiry."""
    # Filter to nearest expiry
    expiry_opts = [o for o in options if o.get("expiry") == nearest_expiry]
    if not expiry_opts:
        return None

    # Get all strikes
    strikes = sorted(set(o["strike"] for o in expiry_opts))
    if not strikes:
        return None

    # For each potential settlement price, compute total loss for option writers
    min_pain = float('inf')
    max_pain_strike = None

    for settle in strikes:
        total_pain = 0
        for o in expiry_opts:
            oi = o.get("oi", 0)
            if oi <= 0:
                continue
            if o["type"] == "call":
                # Call holders' gain = max(0, settle - strike) * OI
                total_pain += max(0, settle - o["strike"]) * oi
            else:
                # Put holders' gain = max(0, strike - settle) * OI
                total_pain += max(0, o["strike"] - settle) * oi

        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = settle

    return max_pain_strike


def process_currency(currency):
    """Process all options for a currency and return summary metrics."""
    print(f"\n  [{currency}] Fetching options summary...")
    data = fetch_options_summary(currency)
    if not data:
        print(f"    No data")
        return None

    price = get_underlying_price(currency)
    print(f"    {len(data)} instruments, underlying: ${price:,.0f}")

    total_oi = 0
    total_volume_usd = 0
    put_oi = 0
    call_oi = 0
    put_volume = 0
    call_volume = 0
    iv_atm_sum = 0
    iv_atm_count = 0

    # For max pain
    parsed_options = []
    expiry_dates = defaultdict(float)

    for item in data:
        parsed = parse_instrument(item["instrument_name"])
        if not parsed:
            continue

        oi = float(item.get("open_interest") or 0)
        vol_usd = float(item.get("volume_usd") or 0)
        mark_iv = item.get("mark_iv")

        total_oi += oi
        total_volume_usd += vol_usd

        if parsed["type"] == "put":
            put_oi += oi
            put_volume += vol_usd
        else:
            call_oi += oi
            call_volume += vol_usd

        # ATM IV: strikes within 5% of current price
        if mark_iv and mark_iv > 0 and abs(parsed["strike"] - price) / price < 0.05:
            iv_atm_sum += mark_iv
            iv_atm_count += 1

        # Track for max pain
        expiry_dates[parsed["expiry"]] += oi
        parsed_options.append({**parsed, "oi": oi})

    # Find nearest expiry (by total OI, excluding very low OI expiries)
    nearest_expiry = None
    if expiry_dates:
        # Sort expiries chronologically
        sorted_expiries = sorted(expiry_dates.keys())
        # Pick the nearest with meaningful OI
        for exp in sorted_expiries:
            if expiry_dates[exp] > 10:
                nearest_expiry = exp
                break

    max_pain = compute_max_pain(parsed_options, nearest_expiry) if nearest_expiry else None

    total_oi_usd = total_oi * price
    pc_ratio_oi = put_oi / call_oi if call_oi > 0 else None
    pc_ratio_vol = put_volume / call_volume if call_volume > 0 else None
    avg_iv = iv_atm_sum / iv_atm_count if iv_atm_count > 0 else None

    print(f"    OI: {total_oi:,.1f} {currency} (${total_oi_usd/1e9:.1f}B)")
    print(f"    Put/Call OI: {pc_ratio_oi:.2f}" if pc_ratio_oi else "    Put/Call OI: N/A")
    print(f"    Volume: ${total_volume_usd/1e6:.1f}M")
    print(f"    ATM IV: {avg_iv:.1f}%" if avg_iv else "    ATM IV: N/A")
    print(f"    Max pain ({nearest_expiry}): ${max_pain:,.0f}" if max_pain else "    Max pain: N/A")

    return {
        "total_oi_contracts": round(total_oi, 4),
        "total_oi_usd": round(total_oi_usd, 2),
        "total_volume_usd": round(total_volume_usd, 2),
        "put_oi": round(put_oi, 4),
        "call_oi": round(call_oi, 4),
        "pc_ratio_oi": round(pc_ratio_oi, 4) if pc_ratio_oi else None,
        "put_volume": round(put_volume, 2),
        "call_volume": round(call_volume, 2),
        "pc_ratio_volume": round(pc_ratio_vol, 4) if pc_ratio_vol else None,
        "max_pain_nearest": max_pain,
        "avg_iv_atm": round(avg_iv, 2) if avg_iv else None,
    }


def main():
    print("━" * 60)
    print("  OPTIONS DAILY SNAPSHOT (Deribit)")
    print("━" * 60)

    create_table()

    conn = get_conn()
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    for currency in ["BTC", "ETH"]:
        metrics = process_currency(currency)
        if not metrics:
            continue

        cur.execute("""
            INSERT INTO options_daily (
                timestamp, currency, total_oi_contracts, total_oi_usd, total_volume_usd,
                put_oi, call_oi, pc_ratio_oi, put_volume, call_volume, pc_ratio_volume,
                max_pain_nearest, avg_iv_atm, source, ingested_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (timestamp, currency) DO UPDATE SET
                total_oi_contracts=EXCLUDED.total_oi_contracts, total_oi_usd=EXCLUDED.total_oi_usd,
                total_volume_usd=EXCLUDED.total_volume_usd, put_oi=EXCLUDED.put_oi, call_oi=EXCLUDED.call_oi,
                pc_ratio_oi=EXCLUDED.pc_ratio_oi, put_volume=EXCLUDED.put_volume, call_volume=EXCLUDED.call_volume,
                pc_ratio_volume=EXCLUDED.pc_ratio_volume, max_pain_nearest=EXCLUDED.max_pain_nearest,
                avg_iv_atm=EXCLUDED.avg_iv_atm, source=EXCLUDED.source, ingested_at=EXCLUDED.ingested_at
        """, (today, currency, metrics["total_oi_contracts"], metrics["total_oi_usd"],
              metrics["total_volume_usd"], metrics["put_oi"], metrics["call_oi"],
              metrics["pc_ratio_oi"], metrics["put_volume"], metrics["call_volume"],
              metrics["pc_ratio_volume"], metrics["max_pain_nearest"], metrics["avg_iv_atm"],
              "deribit", now.strftime("%Y-%m-%dT%H:%M:%SZ")))

    conn.commit()
    conn.close()

    print(f"\n{'━' * 60}")
    print(f"  DONE — snapshot saved for {today}")
    print(f"{'━' * 60}")


if __name__ == "__main__":
    main()
