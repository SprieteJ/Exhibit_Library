#!/usr/bin/env python3
"""
ETH/BTC Ratio → Risk-On Expansion Backtest
───────────────────────────────────────────
Tests whether ETH outperforming BTC predicts broader alt outperformance.

ETH is the bellwether: when ETH/BTC rises, it often signals capital 
rotating from BTC into risk assets. But does it actually predict alts?

Tests:
1. Raw 30d ETH/BTC return thresholds (+3%, +5%, +7%, +10%)
2. Z-scored ETH/BTC 30d return (vs trailing 1yr)
3. ETH/BTC from depressed levels (ratio < 20th percentile + rising)
4. Combined: ETH/BTC rising + dom falling
"""

import os, psycopg2
import pandas as pd
import numpy as np
from datetime import timedelta

DATABASE_URL = os.environ["DATABASE_URL"]

COOLDOWN = 90
FWD_WINDOWS = [30, 60, 90, 180]
ZSCORE_WINDOW = 365


def get_data():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ETH and BTC prices
    cur.execute("""
        SELECT e.timestamp::date as dt, e.price_usd as eth, b.price_usd as btc
        FROM price_daily e
        JOIN price_daily b ON e.timestamp::date = b.timestamp::date
        WHERE e.symbol = 'ETH' AND b.symbol = 'BTC'
          AND e.price_usd > 0 AND b.price_usd > 0
        ORDER BY e.timestamp
    """)
    prices = pd.DataFrame(cur.fetchall(), columns=["date", "eth", "btc"])
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.set_index("date").sort_index()
    prices["eth"] = prices["eth"].astype(float)
    prices["btc"] = prices["btc"].astype(float)

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

    # Funding
    cur.execute("""
        SELECT timestamp::date as dt, AVG(funding_rate) as fr
        FROM funding_8h WHERE symbol = 'BTC' AND exchange = 'binance'
        GROUP BY timestamp::date ORDER BY timestamp::date
    """)
    fund = pd.DataFrame(cur.fetchall(), columns=["date", "funding"])
    fund["date"] = pd.to_datetime(fund["date"])
    fund = fund.set_index("date").sort_index()
    fund["funding"] = fund["funding"].astype(float)

    conn.close()

    df = prices.join(alt, how="inner").join(dom, how="left").join(fund, how="left")
    df = df.dropna(subset=["eth", "btc", "alt_mcap"])
    return df


def compute_features(df):
    # ETH/BTC ratio
    df["ratio"] = df["eth"] / df["btc"]

    # 30d return of ratio
    df["ratio_30d_ret"] = df["ratio"] / df["ratio"].shift(30) - 1
    df["ratio_14d_ret"] = df["ratio"] / df["ratio"].shift(14) - 1
    df["ratio_7d_ret"] = df["ratio"] / df["ratio"].shift(7) - 1

    # Z-score of 30d return
    df["ratio_ret_mean"] = df["ratio_30d_ret"].rolling(ZSCORE_WINDOW, min_periods=180).mean()
    df["ratio_ret_std"] = df["ratio_30d_ret"].rolling(ZSCORE_WINDOW, min_periods=180).std()
    df["ratio_z"] = (df["ratio_30d_ret"] - df["ratio_ret_mean"]) / df["ratio_ret_std"]

    # Ratio percentile (trailing 1yr)
    df["ratio_pctl"] = df["ratio"].rolling(ZSCORE_WINDOW, min_periods=180).rank(pct=True)

    # Dominance z-score (for combined tests)
    dom_change = df["dom"] - df["dom"].shift(30)
    df["dom_z"] = (dom_change - dom_change.rolling(ZSCORE_WINDOW, min_periods=180).mean()) \
                / dom_change.rolling(ZSCORE_WINDOW, min_periods=180).std()

    # Funding 7d avg
    df["fr_7d"] = df["funding"].rolling(7, min_periods=3).mean()

    # Forward returns
    for w in FWD_WINDOWS:
        df[f"alt_fwd_{w}"] = df["alt_mcap"].shift(-w) / df["alt_mcap"] - 1
        df[f"btc_fwd_{w}"] = df["btc"].shift(-w) / df["btc"] - 1
        df[f"spread_fwd_{w}"] = df[f"alt_fwd_{w}"] - df[f"btc_fwd_{w}"]
        df[f"eth_fwd_{w}"] = df["eth"].shift(-w) / df["eth"] - 1

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
    print(f"  {'─' * 80}")
    print(f"  Signals: {n} | {signals[0].strftime('%Y-%m-%d')} to {signals[-1].strftime('%Y-%m-%d')}")

    print(f"\n  {'Date':>12s} {'Ratio':>8s} {'30dRet':>8s} {'RatZ':>6s} {'Pctl':>6s} {'Dom':>6s} | {'Alt30d':>8s} {'Alt90d':>8s} {'vsB90d':>8s} {'ETH90d':>8s}")
    print(f"  {'─'*12} {'─'*8} {'─'*8} {'─'*6} {'─'*6} {'─'*6}   {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

    for d in signals:
        r = df.loc[d]
        ratio = r["ratio"]
        ret30 = r["ratio_30d_ret"] * 100 if pd.notna(r["ratio_30d_ret"]) else 0
        rz = r["ratio_z"] if pd.notna(r["ratio_z"]) else 0
        pctl = r["ratio_pctl"] * 100 if pd.notna(r["ratio_pctl"]) else 0
        dom_val = r["dom"] if pd.notna(r["dom"]) else 0

        parts = [f"  {d.strftime('%Y-%m-%d'):>12s}",
                 f"{ratio:8.5f}", f"{ret30:+8.1f}%", f"{rz:+6.2f}",
                 f"{pctl:5.0f}%", f"{dom_val:6.1f}", "|"]

        for col in ["alt_fwd_30", "alt_fwd_90", "spread_fwd_90", "eth_fwd_90"]:
            val = r.get(col)
            parts.append(f"{val*100:+8.1f}%" if pd.notna(val) else f"{'n/a':>8s}")

        print(" ".join(parts))

    # Summary
    print(f"\n  {'Horizon':>8s} | {'Alt Avg':>8s} {'Alt Med':>8s} {'Alt WR':>7s} | {'Sprd Avg':>9s} {'Sprd WR':>8s} | {'ETH Avg':>8s} {'ETH WR':>7s}")
    print(f"  {'─'*8}   {'─'*8} {'─'*8} {'─'*7}   {'─'*9} {'─'*8}   {'─'*8} {'─'*7}")

    for w in FWD_WINDOWS:
        alt_r = np.array([df.loc[d, f"alt_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"alt_fwd_{w}"])]) * 100
        sp_r = np.array([df.loc[d, f"spread_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"spread_fwd_{w}"])]) * 100
        eth_r = np.array([df.loc[d, f"eth_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"eth_fwd_{w}"])]) * 100

        if len(alt_r) > 0:
            print(f"  {w:>6d}d | {np.mean(alt_r):+8.1f}% {np.median(alt_r):+8.1f}% {(alt_r>0).mean()*100:6.0f}% | {np.mean(sp_r):+9.1f}pp {(sp_r>0).mean()*100:7.0f}% | {np.mean(eth_r):+8.1f}% {(eth_r>0).mean()*100:6.0f}%")


def main():
    print("━" * 80)
    print("  ETH/BTC RATIO → RISK-ON EXPANSION BACKTEST")
    print("━" * 80)

    df = get_data()
    print(f"\n  Data: {len(df)} days, {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")

    df = compute_features(df)

    # Distribution
    ret30 = df["ratio_30d_ret"].dropna() * 100
    print(f"\n  ETH/BTC 30d return distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(ret30, p):+.1f}%")

    rz = df["ratio_z"].dropna()
    print(f"\n  ETH/BTC 30d return z-score distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(rz, p):+.2f}")

    # Unconditional
    print(f"\n  ── UNCONDITIONAL baseline ──")
    for w in FWD_WINDOWS:
        v = df[f"alt_fwd_{w}"].dropna() * 100
        s = df[f"spread_fwd_{w}"].dropna() * 100
        e = df[f"eth_fwd_{w}"].dropna() * 100
        print(f"    {w:>3d}d: alt avg {v.mean():+.1f}% WR {(v>0).mean()*100:.0f}% | spread {s.mean():+.1f}pp WR {(s>0).mean()*100:.0f}% | ETH {e.mean():+.1f}% WR {(e>0).mean()*100:.0f}%")

    # ── Test 1: Raw 30d ETH/BTC return thresholds ──
    for thresh in [3, 5, 7, 10, 15]:
        mask = df["ratio_30d_ret"] > thresh / 100
        sig = dedup(df.index[mask].tolist())
        summarize(df, sig, f"TEST 1: ETH/BTC 30d return > +{thresh}%")

    # ── Test 2: Z-score of ETH/BTC return ──
    for z in [0.5, 1.0, 1.5, 2.0]:
        mask = df["ratio_z"] > z
        sig = dedup(df.index[mask].tolist())
        summarize(df, sig, f"TEST 2: ETH/BTC return z > {z:.1f}")

    # ── Test 3: Ratio bouncing from low levels ──
    # Ratio in bottom 20% of trailing year AND 14d return positive
    mask = (df["ratio_pctl"] < 0.2) & (df["ratio_14d_ret"] > 0.03)
    sig = dedup(df.index[mask.fillna(False)].tolist())
    summarize(df, sig, "TEST 3: Ratio in bottom 20% pctl + 14d ret > +3%")

    mask2 = (df["ratio_pctl"] < 0.3) & (df["ratio_14d_ret"] > 0.05)
    sig2 = dedup(df.index[mask2.fillna(False)].tolist())
    summarize(df, sig2, "TEST 3b: Ratio in bottom 30% pctl + 14d ret > +5%")

    # ── Test 4: Combined ETH/BTC rising + dom falling ──
    mask_combo1 = (df["ratio_30d_ret"] > 0.05) & (df["dom_z"] < -1.0)
    sig_c1 = dedup(df.index[mask_combo1.fillna(False)].tolist())
    summarize(df, sig_c1, "TEST 4: ETH/BTC +5% AND dom z < -1.0")

    mask_combo2 = (df["ratio_z"] > 1.0) & (df["dom_z"] < -1.0)
    sig_c2 = dedup(df.index[mask_combo2.fillna(False)].tolist())
    summarize(df, sig_c2, "TEST 4b: ETH/BTC z > 1.0 AND dom z < -1.0")

    # ── Test 5: Triple — ETH/BTC + dom + funding ──
    mask_triple = (df["ratio_30d_ret"] > 0.05) & (df["dom_z"] < -1.0) & (df["fr_7d"] > 0.0001)
    sig_t = dedup(df.index[mask_triple.fillna(False)].tolist())
    summarize(df, sig_t, "TEST 5: ETH/BTC +5% AND dom z < -1 AND FR > 0.01%")

    # ── Comparison ──
    print(f"\n{'━' * 80}")
    print(f"  COMPARISON TABLE (90d forward)")
    print(f"{'━' * 80}")
    print(f"\n  {'Filter':55s} {'#Sig':>5s} | {'Alt90d':>8s} {'WR':>5s} | {'Sprd90d':>9s} {'WR':>5s} | {'ETH90d':>8s}")
    print(f"  {'─'*55} {'─'*5}   {'─'*8} {'─'*5}   {'─'*9} {'─'*5}   {'─'*8}")

    all_tests = [
        ("ETH/BTC 30d > +3%", df["ratio_30d_ret"] > 0.03),
        ("ETH/BTC 30d > +5%", df["ratio_30d_ret"] > 0.05),
        ("ETH/BTC 30d > +10%", df["ratio_30d_ret"] > 0.10),
        ("ETH/BTC z > 0.5", df["ratio_z"] > 0.5),
        ("ETH/BTC z > 1.0", df["ratio_z"] > 1.0),
        ("ETH/BTC z > 1.5", df["ratio_z"] > 1.5),
        ("Bottom 20% pctl + 14d > +3%", (df["ratio_pctl"] < 0.2) & (df["ratio_14d_ret"] > 0.03)),
        ("Bottom 30% pctl + 14d > +5%", (df["ratio_pctl"] < 0.3) & (df["ratio_14d_ret"] > 0.05)),
        ("ETH/BTC +5% AND dom z < -1", (df["ratio_30d_ret"] > 0.05) & (df["dom_z"] < -1.0)),
        ("ETH/BTC z>1 AND dom z < -1", (df["ratio_z"] > 1.0) & (df["dom_z"] < -1.0)),
        ("Triple: ETH/BTC+dom+funding", (df["ratio_30d_ret"] > 0.05) & (df["dom_z"] < -1.0) & (df["fr_7d"] > 0.0001)),
    ]

    for label, mask in all_tests:
        mask = mask.fillna(False)
        sigs = dedup(df.index[mask].tolist())
        if not sigs:
            print(f"  {label:55s} {0:>5d} |      –     – |         –     – |        –")
            continue
        a90 = np.array([df.loc[d, "alt_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "alt_fwd_90"])]) * 100
        s90 = np.array([df.loc[d, "spread_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "spread_fwd_90"])]) * 100
        e90 = np.array([df.loc[d, "eth_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "eth_fwd_90"])]) * 100
        print(f"  {label:55s} {len(sigs):>5d} | {np.mean(a90):+8.1f}% {(a90>0).mean()*100:4.0f}% | {np.mean(s90):+9.1f}pp {(s90>0).mean()*100:4.0f}% | {np.mean(e90):+8.1f}%")

    uncond_a = df["alt_fwd_90"].dropna() * 100
    uncond_s = df["spread_fwd_90"].dropna() * 100
    uncond_e = df["eth_fwd_90"].dropna() * 100
    print(f"\n  {'Unconditional':55s}       | {uncond_a.mean():+8.1f}% {(uncond_a>0).mean()*100:4.0f}% | {uncond_s.mean():+9.1f}pp {(uncond_s>0).mean()*100:4.0f}% | {uncond_e.mean():+8.1f}%")

    print(f"\n{'━' * 80}")
    print(f"  DONE")
    print(f"{'━' * 80}")


if __name__ == "__main__":
    main()
