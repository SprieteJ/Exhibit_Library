#!/usr/bin/env python3
"""
BTC Dominance + Volatility/Funding Combined Signal Backtest
───────────────────────────────────────────────────────────
Tests whether combining dominance drop with vol/funding filters
improves signal quality for predicting risk-on expansion.

Filters tested:
A) Dom z < -1.0 alone (baseline)
B) Dom z < -1.0 AND RV30d < 60%
C) Dom z < -1.0 AND funding > 0
D) Dom z < -1.0 AND RV30d < 60% AND funding > 0
E) Dom z < -1.0 AND RV30d < 60% OR funding > 0 (at least one confirms)
"""

import os, math, psycopg2, psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

DATABASE_URL = os.environ["DATABASE_URL"]

LOOKBACK = 30
ZSCORE_WINDOW = 365
COOLDOWN = 90
FWD_WINDOWS = [30, 60, 90, 180]


def get_data():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

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

    # BTC price (for vol and forward returns)
    cur.execute("""
        SELECT timestamp::date as dt, price_usd
        FROM price_daily WHERE symbol = 'BTC' AND price_usd > 0
        ORDER BY timestamp
    """)
    btc = pd.DataFrame(cur.fetchall(), columns=["date", "btc"])
    btc["date"] = pd.to_datetime(btc["date"])
    btc = btc.set_index("date").sort_index()
    btc["btc"] = btc["btc"].astype(float)

    # Funding rate (daily avg from 8h data)
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

    conn.close()

    df = dom.join(alt, how="inner").join(btc, how="inner").join(fund, how="left")
    df = df.dropna(subset=["dom", "alt_mcap", "btc"])
    return df


def compute_features(df):
    # Dominance z-score
    df["dom_change"] = df["dom"] - df["dom"].shift(LOOKBACK)
    df["dom_z"] = (df["dom_change"] - df["dom_change"].rolling(ZSCORE_WINDOW, min_periods=180).mean()) \
                / df["dom_change"].rolling(ZSCORE_WINDOW, min_periods=180).std()

    # 30d realized vol (annualized)
    log_ret = np.log(df["btc"] / df["btc"].shift(1))
    df["rv30"] = log_ret.rolling(30, min_periods=20).std() * np.sqrt(365) * 100

    # 7d avg funding rate
    df["funding_7d"] = df["funding"].rolling(7, min_periods=3).mean()

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
        print(f"  {label:50s} | {'NO SIGNALS':>60s}")
        return

    # Collect stats
    stats = {}
    for w in FWD_WINDOWS:
        alt_rets = [df.loc[d, f"alt_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"alt_fwd_{w}"])]
        spreads = [df.loc[d, f"spread_fwd_{w}"] for d in signals if d in df.index and pd.notna(df.loc[d, f"spread_fwd_{w}"])]
        if alt_rets:
            alt_rets = np.array(alt_rets) * 100
            spreads = np.array(spreads) * 100
            stats[w] = {
                "alt_avg": np.mean(alt_rets),
                "alt_med": np.median(alt_rets),
                "alt_wr": (alt_rets > 0).mean() * 100,
                "spread_avg": np.mean(spreads),
                "spread_med": np.median(spreads),
                "spread_wr": (spreads > 0).mean() * 100,
            }

    # Date range
    date_range = f"{signals[0].strftime('%Y-%m-%d')} to {signals[-1].strftime('%Y-%m-%d')}"

    print(f"\n  {label}")
    print(f"  {'─' * 75}")
    print(f"  Signals: {n} | {date_range}")

    # Per-signal detail
    print(f"\n  {'Date':>12s} {'Dom':>6s} {'DomZ':>6s} {'RV30':>6s} {'Fund7d':>8s} | {'Alt30d':>8s} {'Alt60d':>8s} {'Alt90d':>8s} {'vsB90d':>8s}")
    print(f"  {'─'*12} {'─'*6} {'─'*6} {'─'*6} {'─'*8}   {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

    for d in signals:
        r = df.loc[d]
        dom_val = r["dom"]
        dom_z = r["dom_z"] if pd.notna(r["dom_z"]) else 0
        rv = r["rv30"] if pd.notna(r["rv30"]) else 0
        fund = r["funding_7d"] * 100 if pd.notna(r["funding_7d"]) else float('nan')

        parts = [f"  {d.strftime('%Y-%m-%d'):>12s}", f"{dom_val:6.1f}", f"{dom_z:+6.2f}",
                 f"{rv:6.1f}", f"{fund:+8.4f}" if not np.isnan(fund) else f"{'n/a':>8s}", "|"]

        for w in [30, 60, 90]:
            val = r.get(f"alt_fwd_{w}")
            parts.append(f"{val*100:+8.1f}%" if pd.notna(val) else f"{'n/a':>8s}")

        spread = r.get("spread_fwd_90")
        parts.append(f"{spread*100:+8.1f}%" if pd.notna(spread) else f"{'n/a':>8s}")

        print(" ".join(parts))

    # Summary table
    print(f"\n  {'Horizon':>8s} | {'Alt Avg':>8s} {'Alt Med':>8s} {'Alt WR':>7s} | {'Sprd Avg':>9s} {'Sprd Med':>9s} {'Sprd WR':>8s}")
    print(f"  {'─'*8}   {'─'*8} {'─'*8} {'─'*7}   {'─'*9} {'─'*9} {'─'*8}")

    for w in FWD_WINDOWS:
        if w in stats:
            s = stats[w]
            print(f"  {w:>6d}d | {s['alt_avg']:+8.1f}% {s['alt_med']:+8.1f}% {s['alt_wr']:6.0f}% | {s['spread_avg']:+9.1f}pp {s['spread_med']:+9.1f}pp {s['spread_wr']:7.0f}%")

    return stats


def main():
    print("━" * 78)
    print("  BTC DOMINANCE + FILTERS — COMBINED SIGNAL BACKTEST")
    print("━" * 78)

    df = get_data()
    print(f"\n  Data: {len(df)} days, {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")

    df = compute_features(df)

    # Funding data availability
    fund_start = df["funding"].dropna().index[0] if df["funding"].notna().any() else None
    print(f"  Funding data from: {fund_start.strftime('%Y-%m-%d') if fund_start else 'N/A'}")
    print(f"  RV data from: {df['rv30'].dropna().index[0].strftime('%Y-%m-%d')}")

    # Unconditional baseline
    print(f"\n  ── UNCONDITIONAL (random date) baseline ──")
    uncond = {}
    for w in FWD_WINDOWS:
        v = df[f"alt_fwd_{w}"].dropna() * 100
        s = df[f"spread_fwd_{w}"].dropna() * 100
        uncond[w] = {"alt_avg": v.mean(), "alt_wr": (v > 0).mean() * 100,
                     "spread_avg": s.mean(), "spread_wr": (s > 0).mean() * 100}
        print(f"    {w:>3d}d: alt avg {v.mean():+.1f}%, WR {(v>0).mean()*100:.0f}% | spread avg {s.mean():+.1f}pp, WR {(s>0).mean()*100:.0f}%")

    # ── FILTER A: Dom z < -1.0 alone ──
    mask_a = df["dom_z"] < -1.0
    sig_a = dedup(df.index[mask_a].tolist())
    summarize(df, sig_a, "FILTER A: Dom z < -1.0 (baseline)")

    # ── FILTER B: Dom z < -1.0 AND RV < 60% ──
    mask_b = (df["dom_z"] < -1.0) & (df["rv30"] < 60)
    sig_b = dedup(df.index[mask_b].tolist())
    summarize(df, sig_b, "FILTER B: Dom z < -1.0 AND RV30 < 60%")

    # ── FILTER C: Dom z < -1.0 AND RV < 50% ──
    mask_c = (df["dom_z"] < -1.0) & (df["rv30"] < 50)
    sig_c = dedup(df.index[mask_c].tolist())
    summarize(df, sig_c, "FILTER C: Dom z < -1.0 AND RV30 < 50%")

    # ── FILTER D: Dom z < -1.0 AND funding > 0 ──
    mask_d = (df["dom_z"] < -1.0) & (df["funding_7d"] > 0)
    sig_d = dedup(df.index[mask_d].tolist())
    summarize(df, sig_d, "FILTER D: Dom z < -1.0 AND funding > 0")

    # ── FILTER E: Dom z < -1.0 AND (RV < 60% AND funding > 0) ──
    mask_e = (df["dom_z"] < -1.0) & (df["rv30"] < 60) & (df["funding_7d"] > 0)
    sig_e = dedup(df.index[mask_e].tolist())
    summarize(df, sig_e, "FILTER E: Dom z < -1.0 AND RV30 < 60% AND funding > 0")

    # ── FILTER F: Dom z < -1.0 AND (RV < 60% OR funding > 0) ──
    mask_f = (df["dom_z"] < -1.0) & ((df["rv30"] < 60) | (df["funding_7d"] > 0))
    sig_f = dedup(df.index[mask_f].tolist())
    summarize(df, sig_f, "FILTER F: Dom z < -1.0 AND (RV < 60% OR funding > 0)")

    # ── FILTER G: Dom z < -1.0 AND RV < 60% AND dom_level < 65% ──
    # (high dom starting point means less room for alt rotation)
    mask_g = (df["dom_z"] < -1.0) & (df["rv30"] < 60) & (df["dom"] < 65)
    sig_g = dedup(df.index[mask_g].tolist())
    summarize(df, sig_g, "FILTER G: Dom z < -1.0 AND RV < 60% AND dom < 65%")

    # ── Summary comparison ──
    print(f"\n{'━' * 78}")
    print(f"  COMPARISON TABLE")
    print(f"{'━' * 78}")
    print(f"\n  {'Filter':50s} {'#Sig':>5s} | {'Alt90d':>8s} {'WR':>5s} | {'Sprd90d':>9s} {'WR':>5s}")
    print(f"  {'─'*50} {'─'*5}   {'─'*8} {'─'*5}   {'─'*9} {'─'*5}")

    all_results = [
        ("A: Dom z < -1.0", sig_a),
        ("B: + RV < 60%", sig_b),
        ("C: + RV < 50%", sig_c),
        ("D: + funding > 0", sig_d),
        ("E: + RV < 60% + funding > 0", sig_e),
        ("F: + RV < 60% OR funding > 0", sig_f),
        ("G: + RV < 60% + dom < 65%", sig_g),
    ]

    for label, sigs in all_results:
        if not sigs:
            print(f"  {label:50s} {0:>5d} | {'–':>8s} {'–':>5s} | {'–':>9s} {'–':>5s}")
            continue
        alt90 = [df.loc[d, "alt_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "alt_fwd_90"])]
        sp90 = [df.loc[d, "spread_fwd_90"] for d in sigs if d in df.index and pd.notna(df.loc[d, "spread_fwd_90"])]
        if alt90:
            a = np.array(alt90) * 100
            s = np.array(sp90) * 100
            print(f"  {label:50s} {len(sigs):>5d} | {np.mean(a):+8.1f}% {(a>0).mean()*100:4.0f}% | {np.mean(s):+9.1f}pp {(s>0).mean()*100:4.0f}%")

    print(f"\n  Unconditional baseline:")
    print(f"  {'Random date':50s}       | {uncond[90]['alt_avg']:+8.1f}% {uncond[90]['alt_wr']:4.0f}% | {uncond[90]['spread_avg']:+9.1f}pp {uncond[90]['spread_wr']:4.0f}%")

    print(f"\n{'━' * 78}")
    print(f"  DONE")
    print(f"{'━' * 78}")


if __name__ == "__main__":
    main()
