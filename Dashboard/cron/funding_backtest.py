#!/usr/bin/env python3
"""
Funding Rate → Risk-On Expansion Backtest
─────────────────────────────────────────
Tests whether sustained positive funding predicts alt outperformance.

Approach:
- Compute 7d avg funding rate
- Z-score it against trailing 1-year distribution
- Test multiple thresholds: raw level AND z-score
- Also test: funding positive for N consecutive days
- Measure forward alt returns + alt vs BTC spread
- Compare against unconditional baseline

Key question: does positive funding mean "speculation is healthy and alts 
will outperform" or does it mean "market is overheated and about to dump"?
"""

import os, math, psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

DATABASE_URL = os.environ["DATABASE_URL"]

COOLDOWN = 90
FWD_WINDOWS = [30, 60, 90, 180]
ZSCORE_WINDOW = 365


def get_data():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Daily avg funding rate
    cur.execute("""
        SELECT timestamp::date as dt, AVG(funding_rate) as fr
        FROM funding_8h
        WHERE symbol = 'BTC' AND exchange = 'binance'
        GROUP BY timestamp::date
        ORDER BY timestamp::date
    """)
    fund = pd.DataFrame(cur.fetchall(), columns=["date", "funding"])
    fund["date"] = pd.to_datetime(fund["date"])
    fund = fund.set_index("date").sort_index()
    fund["funding"] = fund["funding"].astype(float)

    # Alt mcap
    cur.execute("""
        SELECT t.timestamp::date as dt,
               t.total_mcap_usd - b.market_cap_usd as alt_mcap
        FROM total_marketcap_daily t
        JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
        ORDER BY t.timestamp
    """)
    alt = pd.DataFrame(cur.fetchall(), columns=["date", "alt_mcap"])
    alt["date"] = pd.to_datetime(alt["date"])
    alt = alt.set_index("date").sort_index()
    alt["alt_mcap"] = alt["alt_mcap"].astype(float)

    # BTC price
    cur.execute("""
        SELECT timestamp::date as dt, price_usd
        FROM price_daily WHERE symbol = 'BTC' AND price_usd > 0
        ORDER BY timestamp
    """)
    btc = pd.DataFrame(cur.fetchall(), columns=["date", "btc"])
    btc["date"] = pd.to_datetime(btc["date"])
    btc = btc.set_index("date").sort_index()
    btc["btc"] = btc["btc"].astype(float)

    # BTC dominance
    cur.execute("""
        SELECT b.timestamp::date as dt,
               b.market_cap_usd / t.total_mcap_usd * 100 as dom
        FROM marketcap_daily b
        JOIN total_marketcap_daily t ON b.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
        ORDER BY b.timestamp
    """)
    dom = pd.DataFrame(cur.fetchall(), columns=["date", "dom"])
    dom["date"] = pd.to_datetime(dom["date"])
    dom = dom.set_index("date").sort_index()
    dom["dom"] = dom["dom"].astype(float)

    # RV for context
    cur.execute("""
        SELECT timestamp::date as dt, price_usd
        FROM price_daily WHERE symbol = 'BTC' AND price_usd > 0
        ORDER BY timestamp
    """)
    p = pd.DataFrame(cur.fetchall(), columns=["date", "p"])
    p["date"] = pd.to_datetime(p["date"])
    p = p.set_index("date").sort_index()
    log_ret = np.log(p["p"] / p["p"].shift(1))
    rv = log_ret.rolling(30, min_periods=20).std() * np.sqrt(365) * 100
    rv = rv.rename("rv30")

    conn.close()

    df = fund.join(alt, how="inner").join(btc, how="inner").join(dom, how="left").join(rv, how="left")
    df = df.dropna(subset=["funding", "alt_mcap", "btc"])
    return df


def compute_features(df):
    # Rolling averages of funding
    df["fr_7d"] = df["funding"].rolling(7, min_periods=5).mean()
    df["fr_14d"] = df["funding"].rolling(14, min_periods=10).mean()
    df["fr_30d"] = df["funding"].rolling(30, min_periods=20).mean()

    # Z-score of 7d funding against trailing 1 year
    df["fr_z"] = (df["fr_7d"] - df["fr_7d"].rolling(ZSCORE_WINDOW, min_periods=180).mean()) \
               / df["fr_7d"].rolling(ZSCORE_WINDOW, min_periods=180).std()

    # Consecutive positive funding days
    positive = (df["funding"] > 0).astype(int)
    df["consec_pos"] = positive.groupby((positive != positive.shift()).cumsum()).cumsum()

    # Annualized funding (for intuitive reading)
    df["fr_ann"] = df["fr_7d"] * 3 * 365 * 100  # 3x per day, 365 days, * 100 for %

    # Forward returns
    for w in FWD_WINDOWS:
        df[f"alt_fwd_{w}"] = df["alt_mcap"].shift(-w) / df["alt_mcap"] - 1
        df[f"btc_fwd_{w}"] = df["btc"].shift(-w) / df["btc"] - 1
        df[f"spread_fwd_{w}"] = df[f"alt_fwd_{w}"] - df[f"btc_fwd_{w}"]

    return df


def dedup(dates, cooldown=COOLDOWN):
    filtered = []
    for d in dates:
        if not filtered or (d - filtered[-1]).days >= cooldown:
            filtered.append(d)
    return filtered


def summarize(df, signals, label):
    n = len(signals)
    if n == 0:
        print(f"\n  {label}: NO SIGNALS\n")
        return

    print(f"\n  {label}")
    print(f"  {'─' * 75}")
    print(f"  Signals: {n} | {signals[0].strftime('%Y-%m-%d')} to {signals[-1].strftime('%Y-%m-%d')}")

    # Per-signal detail
    print(f"\n  {'Date':>12s} {'FR7d':>8s} {'FRAnn':>7s} {'FRz':>6s} {'Dom':>6s} {'RV30':>6s} | {'Alt30d':>8s} {'Alt90d':>8s} {'vsB90d':>8s}")
    print(f"  {'─'*12} {'─'*8} {'─'*7} {'─'*6} {'─'*6} {'─'*6}   {'─'*8} {'─'*8} {'─'*8}")

    for d in signals:
        r = df.loc[d]
        fr7 = r["fr_7d"] * 100 if pd.notna(r["fr_7d"]) else 0
        ann = r["fr_ann"] if pd.notna(r["fr_ann"]) else 0
        frz = r["fr_z"] if pd.notna(r["fr_z"]) else 0
        dom_val = r["dom"] if pd.notna(r["dom"]) else 0
        rv = r["rv30"] if pd.notna(r["rv30"]) else 0

        parts = [f"  {d.strftime('%Y-%m-%d'):>12s}",
                 f"{fr7:+8.4f}%", f"{ann:+7.1f}%", f"{frz:+6.2f}",
                 f"{dom_val:6.1f}", f"{rv:6.1f}", "|"]

        for w in [30, 90]:
            val = r.get(f"alt_fwd_{w}")
            parts.append(f"{val*100:+8.1f}%" if pd.notna(val) else f"{'n/a':>8s}")

        sp = r.get("spread_fwd_90")
        parts.append(f"{sp*100:+8.1f}%" if pd.notna(sp) else f"{'n/a':>8s}")

        print(" ".join(parts))

    # Summary
    print(f"\n  {'Horizon':>8s} | {'Alt Avg':>8s} {'Alt Med':>8s} {'Alt WR':>7s} | {'Sprd Avg':>9s} {'Sprd Med':>9s} {'Sprd WR':>8s}")
    print(f"  {'─'*8}   {'─'*8} {'─'*8} {'─'*7}   {'─'*9} {'─'*9} {'─'*8}")

    for w in FWD_WINDOWS:
        alt_r = [df.loc[d, f"alt_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"alt_fwd_{w}"])]
        sp_r = [df.loc[d, f"spread_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"spread_fwd_{w}"])]
        if alt_r:
            a = np.array(alt_r) * 100
            s = np.array(sp_r) * 100
            print(f"  {w:>6d}d | {np.mean(a):+8.1f}% {np.median(a):+8.1f}% {(a>0).mean()*100:6.0f}% | {np.mean(s):+9.1f}pp {np.median(s):+9.1f}pp {(s>0).mean()*100:7.0f}%")


def main():
    print("━" * 78)
    print("  FUNDING RATE → RISK-ON EXPANSION BACKTEST")
    print("━" * 78)

    df = get_data()
    print(f"\n  Data: {len(df)} days, {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")

    df = compute_features(df)

    # Distribution
    fr7 = df["fr_7d"].dropna() * 100
    print(f"\n  7d avg funding rate distribution (%):")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(fr7, p):+.4f}%")

    frz = df["fr_z"].dropna()
    print(f"\n  Funding z-score distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(frz, p):+.2f}")

    # Unconditional
    print(f"\n  ── UNCONDITIONAL baseline ──")
    for w in FWD_WINDOWS:
        v = df[f"alt_fwd_{w}"].dropna() * 100
        s = df[f"spread_fwd_{w}"].dropna() * 100
        print(f"    {w:>3d}d: alt avg {v.mean():+.1f}%, WR {(v>0).mean()*100:.0f}% | spread avg {s.mean():+.1f}pp, WR {(s>0).mean()*100:.0f}%")

    # ── Test 1: Raw funding level thresholds ──
    for thresh in [0.005, 0.01, 0.015, 0.02]:
        mask = df["fr_7d"] > thresh / 100  # thresh is in %, fr_7d is in decimal
        sig = dedup(df.index[mask].tolist())
        summarize(df, sig, f"TEST 1: FR 7d avg > {thresh:.3f}%")

    # ── Test 2: Z-score thresholds ──
    for z_thresh in [0.5, 1.0, 1.5, 2.0]:
        mask = df["fr_z"] > z_thresh
        sig = dedup(df.index[mask].tolist())
        summarize(df, sig, f"TEST 2: FR z-score > {z_thresh:.1f}")

    # ── Test 3: Consecutive positive days ──
    for consec in [7, 14, 21, 30]:
        mask = df["consec_pos"] >= consec
        # Only fire on the first day it hits the threshold
        first_hit = mask & (~mask.shift(1, fill_value=False))
        sig = dedup(df.index[first_hit].tolist())
        summarize(df, sig, f"TEST 3: {consec}+ consecutive positive funding days")

    # ── Test 4: Combined — funding elevated + dominance dropping ──
    dom_change = df["dom"] - df["dom"].shift(30)
    dom_z = (dom_change - dom_change.rolling(365, min_periods=180).mean()) / dom_change.rolling(365, min_periods=180).std()

    mask_combo = (df["fr_7d"] > 0.0001) & (dom_z < -1.0)
    sig_combo = dedup(df.index[mask_combo.fillna(False)].tolist())
    summarize(df, sig_combo, "TEST 4: FR > 0.01% AND dom z < -1.0")

    # ── Comparison table ──
    print(f"\n{'━' * 78}")
    print(f"  COMPARISON TABLE (90d forward)")
    print(f"{'━' * 78}")
    print(f"\n  {'Filter':55s} {'#Sig':>5s} | {'Alt90d':>8s} {'WR':>5s} | {'Sprd90d':>9s} {'WR':>5s}")
    print(f"  {'─'*55} {'─'*5}   {'─'*8} {'─'*5}   {'─'*9} {'─'*5}")

    tests = [
        ("FR 7d > 0.010%", df["fr_7d"] > 0.0001),
        ("FR 7d > 0.015%", df["fr_7d"] > 0.00015),
        ("FR 7d > 0.020%", df["fr_7d"] > 0.0002),
        ("FR z > 0.5", df["fr_z"] > 0.5),
        ("FR z > 1.0", df["fr_z"] > 1.0),
        ("FR z > 1.5", df["fr_z"] > 1.5),
        ("14+ consec positive days", df["consec_pos"] >= 14),
        ("FR > 0.01% AND dom z < -1.0", (df["fr_7d"] > 0.0001) & (dom_z < -1.0)),
    ]

    for label, mask in tests:
        mask = mask.fillna(False)
        if label.endswith("positive days"):
            first_hit = mask & (~mask.shift(1, fill_value=False))
            sigs = dedup(df.index[first_hit].tolist())
        else:
            sigs = dedup(df.index[mask].tolist())

        if not sigs:
            print(f"  {label:55s} {0:>5d} | {'–':>8s} {'–':>5s} | {'–':>9s} {'–':>5s}")
            continue

        a90 = [df.loc[d, "alt_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "alt_fwd_90"])]
        s90 = [df.loc[d, "spread_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "spread_fwd_90"])]
        if a90:
            a = np.array(a90) * 100
            s = np.array(s90) * 100
            print(f"  {label:55s} {len(sigs):>5d} | {np.mean(a):+8.1f}% {(a>0).mean()*100:4.0f}% | {np.mean(s):+9.1f}pp {(s>0).mean()*100:4.0f}%")

    uncond_a = df["alt_fwd_90"].dropna() * 100
    uncond_s = df["spread_fwd_90"].dropna() * 100
    print(f"\n  {'Unconditional baseline':55s}       | {uncond_a.mean():+8.1f}% {(uncond_a>0).mean()*100:4.0f}% | {uncond_s.mean():+9.1f}pp {(uncond_s>0).mean()*100:4.0f}%")

    print(f"\n{'━' * 78}")
    print(f"  DONE")
    print(f"{'━' * 78}")


if __name__ == "__main__":
    main()
