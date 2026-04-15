#!/usr/bin/env python3
"""
Alt Intracorrelation → Risk-On Expansion Backtest
──────────────────────────────────────────────────
Tests whether rising alt intracorrelation predicts alt outperformance.

High intracorrelation = alts moving together.
But moving together UP vs moving together DOWN are very different signals.

Tests:
1. Raw intracorrelation level (> 0.3, 0.4, 0.5, 0.6)
2. Rising intracorrelation (30d change)
3. High intracorrelation + positive alt returns (alts correlated AND going up)
4. High intracorrelation + negative alt returns (alts correlated AND going down = deleveraging)
"""

import os, psycopg2
import pandas as pd
import numpy as np
from datetime import timedelta

DATABASE_URL = os.environ["DATABASE_URL"]

COOLDOWN = 90
FWD_WINDOWS = [30, 60, 90, 180]


def get_data():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Alt intracorrelation
    cur.execute("""
        SELECT timestamp::date as dt, tier, avg_corr
        FROM alt_intracorr_daily
        WHERE avg_corr IS NOT NULL
        ORDER BY timestamp
    """)
    rows = cur.fetchall()

    # Average across tiers for a single reading per day
    corr_data = {}
    for r in rows:
        d = str(r[0])
        if d not in corr_data:
            corr_data[d] = []
        if r[2] is not None:
            corr_data[d].append(float(r[2]))

    corr_series = {}
    for d, vals in corr_data.items():
        if vals:
            corr_series[d] = sum(vals) / len(vals)

    corr_df = pd.DataFrame.from_dict(corr_series, orient='index', columns=['intracorr'])
    corr_df.index = pd.to_datetime(corr_df.index)
    corr_df = corr_df.sort_index()

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
        FROM price_daily WHERE symbol = 'BTC' AND price_usd > 0 ORDER BY timestamp
    """)
    btc = pd.DataFrame(cur.fetchall(), columns=["date", "btc"])
    btc["date"] = pd.to_datetime(btc["date"])
    btc = btc.set_index("date").sort_index()
    btc["btc"] = btc["btc"].astype(float)

    conn.close()

    df = corr_df.join(alt, how="inner").join(btc, how="inner")
    df = df.dropna(subset=["intracorr", "alt_mcap", "btc"])
    return df


def compute_features(df):
    # 30d change in intracorrelation
    df["avg_corr_change"] = df["intracorr"] - df["intracorr"].shift(30)

    # Alt mcap 30d return (to determine if alts are going up or down)
    df["alt_30d_ret"] = df["alt_mcap"] / df["alt_mcap"].shift(30) - 1

    # BTC 30d return
    df["btc_30d_ret"] = df["btc"] / df["btc"].shift(30) - 1

    # Forward returns
    for w in FWD_WINDOWS:
        df[f"alt_fwd_{w}"] = df["alt_mcap"].shift(-w) / df["alt_mcap"] - 1
        df[f"btc_fwd_{w}"] = df["btc"].shift(-w) / df["btc"] - 1
        df[f"spread_fwd_{w}"] = df[f"alt_fwd_{w}"] - df[f"btc_fwd_{w}"]

    return df


def dedup(dates):
    filtered = []
    for d in dates:
        if not filtered or (d - filtered[-1]).days >= COOLDOWN:
            filtered.append(d)
    return filtered


def summarize(df, sigs, label):
    n = len(sigs)
    if n == 0:
        print(f"\n  {label}: NO SIGNALS\n")
        return
    print(f"\n  {label}")
    print(f"  {'─' * 75}")
    print(f"  Signals: {n} | {sigs[0].date()} to {sigs[-1].date()}")

    print(f"\n  {'Date':>12s} {'Corr':>6s} {'30dΔ':>7s} {'AltRet':>8s} | {'Alt30d':>8s} {'Alt90d':>8s} {'vsB90d':>8s}")
    print(f"  {'─'*12} {'─'*6} {'─'*7} {'─'*8}   {'─'*8} {'─'*8} {'─'*8}")
    for d in sigs:
        r = df.loc[d]
        corr = r["intracorr"]
        delta = r["avg_corr_change"] if pd.notna(r["avg_corr_change"]) else 0
        alt_ret = r["alt_30d_ret"] * 100 if pd.notna(r["alt_30d_ret"]) else 0
        parts = [f"  {d.date()!s:>12s}", f"{corr:6.3f}", f"{delta:+7.3f}", f"{alt_ret:+8.1f}%", "|"]
        for col in ["alt_fwd_30", "alt_fwd_90", "spread_fwd_90"]:
            val = r.get(col)
            parts.append(f"{val*100:+8.1f}%" if pd.notna(val) else f"{'n/a':>8s}")
        print(" ".join(parts))

    print(f"\n  {'Horizon':>8s} | {'Alt Avg':>8s} {'Alt Med':>8s} {'Alt WR':>7s} | {'Sprd Avg':>9s} {'Sprd WR':>8s}")
    print(f"  {'─'*8}   {'─'*8} {'─'*8} {'─'*7}   {'─'*9} {'─'*8}")
    for w in FWD_WINDOWS:
        a = np.array([df.loc[d, f"alt_fwd_{w}"] for d in sigs if d in df.index and pd.notna(df.loc[d, f"alt_fwd_{w}"])]) * 100
        s = np.array([df.loc[d, f"spread_fwd_{w}"] for d in sigs if d in df.index and pd.notna(df.loc[d, f"spread_fwd_{w}"])]) * 100
        if len(a) > 0:
            print(f"  {w:>6d}d | {np.mean(a):+8.1f}% {np.median(a):+8.1f}% {(a>0).mean()*100:6.0f}% | {np.mean(s):+9.1f}pp {(s>0).mean()*100:7.0f}%")


def main():
    print("━" * 78)
    print("  ALT INTRACORRELATION → RISK-ON EXPANSION BACKTEST")
    print("━" * 78)

    df = get_data()
    print(f"\n  Data: {len(df)} days, {df.index[0].date()} to {df.index[-1].date()}")

    df = compute_features(df)

    # Distribution
    print(f"\n  Intracorrelation distribution:")
    vals = df["intracorr"].dropna()
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(vals, p):.3f}")

    # Unconditional
    print(f"\n  ── UNCONDITIONAL baseline ──")
    for w in FWD_WINDOWS:
        v = df[f"alt_fwd_{w}"].dropna() * 100
        s = df[f"spread_fwd_{w}"].dropna() * 100
        print(f"    {w:>3d}d: alt avg {v.mean():+.1f}% WR {(v>0).mean()*100:.0f}% | spread {s.mean():+.1f}pp WR {(s>0).mean()*100:.0f}%")

    # ── Test 1: Raw level ──
    for thresh in [0.3, 0.4, 0.5, 0.6]:
        mask = df["intracorr"] > thresh
        sigs = dedup(df.index[mask].tolist())
        summarize(df, sigs, f"TEST 1: Intracorr > {thresh:.1f}")

    # ── Test 2: Rising intracorrelation ──
    for delta in [0.05, 0.10, 0.15, 0.20]:
        mask = df["avg_corr_change"] > delta
        sigs = dedup(df.index[mask.fillna(False)].tolist())
        summarize(df, sigs, f"TEST 2: Intracorr 30d change > +{delta:.2f}")

    # ── Test 3: High corr + positive alt returns (risk-on) ──
    for thresh in [0.3, 0.4, 0.5]:
        mask = (df["intracorr"] > thresh) & (df["alt_30d_ret"] > 0)
        sigs = dedup(df.index[mask.fillna(False)].tolist())
        summarize(df, sigs, f"TEST 3: Intracorr > {thresh:.1f} AND alts up 30d")

    # ── Test 4: High corr + negative alt returns (deleveraging) ──
    for thresh in [0.4, 0.5, 0.6]:
        mask = (df["intracorr"] > thresh) & (df["alt_30d_ret"] < -0.05)
        sigs = dedup(df.index[mask.fillna(False)].tolist())
        summarize(df, sigs, f"TEST 4: Intracorr > {thresh:.1f} AND alts down >5%")

    # ── Test 5: Rising corr + alts outperforming BTC ──
    mask5 = (df["avg_corr_change"] > 0.05) & (df["alt_30d_ret"] > df["btc_30d_ret"])
    sigs5 = dedup(df.index[mask5.fillna(False)].tolist())
    summarize(df, sigs5, "TEST 5: Corr rising +0.05 AND alts outperforming BTC 30d")

    # ── Comparison ──
    print(f"\n{'━' * 78}")
    print(f"  COMPARISON TABLE (90d forward)")
    print(f"{'━' * 78}")
    print(f"\n  {'Filter':55s} {'#Sig':>5s} | {'Alt90d':>8s} {'WR':>5s} | {'Sprd90d':>9s} {'WR':>5s}")
    print(f"  {'─'*55} {'─'*5}   {'─'*8} {'─'*5}   {'─'*9} {'─'*5}")

    tests = [
        ("Intracorr > 0.3", df["intracorr"] > 0.3),
        ("Intracorr > 0.4", df["intracorr"] > 0.4),
        ("Intracorr > 0.5", df["intracorr"] > 0.5),
        ("Intracorr > 0.6", df["intracorr"] > 0.6),
        ("Corr rising > +0.05", df["avg_corr_change"] > 0.05),
        ("Corr rising > +0.10", df["avg_corr_change"] > 0.10),
        ("Corr rising > +0.15", df["avg_corr_change"] > 0.15),
        ("Corr > 0.3 AND alts up", (df["intracorr"] > 0.3) & (df["alt_30d_ret"] > 0)),
        ("Corr > 0.4 AND alts up", (df["intracorr"] > 0.4) & (df["alt_30d_ret"] > 0)),
        ("Corr > 0.5 AND alts up", (df["intracorr"] > 0.5) & (df["alt_30d_ret"] > 0)),
        ("Corr > 0.4 AND alts down >5%", (df["intracorr"] > 0.4) & (df["alt_30d_ret"] < -0.05)),
        ("Corr > 0.5 AND alts down >5%", (df["intracorr"] > 0.5) & (df["alt_30d_ret"] < -0.05)),
        ("Corr rising AND alts > BTC", (df["avg_corr_change"] > 0.05) & (df["alt_30d_ret"] > df["btc_30d_ret"])),
    ]

    for label, mask in tests:
        mask = mask.fillna(False)
        sigs = dedup(df.index[mask].tolist())
        if not sigs:
            print(f"  {label:55s} {0:>5d} |      –     – |         –     –")
            continue
        a90 = np.array([df.loc[d, "alt_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "alt_fwd_90"])]) * 100
        s90 = np.array([df.loc[d, "spread_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "spread_fwd_90"])]) * 100
        if len(a90) > 0:
            print(f"  {label:55s} {len(sigs):>5d} | {np.mean(a90):+8.1f}% {(a90>0).mean()*100:4.0f}% | {np.mean(s90):+9.1f}pp {(s90>0).mean()*100:4.0f}%")

    uncond_a = df["alt_fwd_90"].dropna() * 100
    uncond_s = df["spread_fwd_90"].dropna() * 100
    print(f"\n  {'Unconditional':55s}       | {uncond_a.mean():+8.1f}% {(uncond_a>0).mean()*100:4.0f}% | {uncond_s.mean():+9.1f}pp {(uncond_s>0).mean()*100:4.0f}%")

    print(f"\n{'━' * 78}")
    print(f"  DONE")
    print(f"{'━' * 78}")


if __name__ == "__main__":
    main()
