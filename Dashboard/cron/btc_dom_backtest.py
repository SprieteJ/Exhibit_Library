#!/usr/bin/env python3
"""
BTC Dominance → Risk-On Expansion Backtest
──────────────────────────────────────────
Tests whether BTC dominance drops predict alt outperformance.

Methodology:
1. Compute rolling 30d change in BTC dominance
2. Z-score it against trailing 1-year distribution (self-calibrating)
3. Generate signals when z-score crosses below threshold
4. Measure forward alt returns at 30/60/90/180 days
5. Compare against unconditional (random date) returns
6. Compute hit rates, avg/median returns, Sharpe-like ratio
7. Output a clean summary table

Usage:
  DATABASE_URL="..." python3 btc_dom_backtest.py
"""

import os, math, psycopg2, psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

DATABASE_URL = os.environ["DATABASE_URL"]

# ── Config ──
LOOKBACK_DAYS = 30          # Dominance change window
ZSCORE_WINDOW = 365         # Z-score trailing window (1 year)
SIGNAL_COOLDOWN = 90        # Min days between signals
FORWARD_WINDOWS = [30, 60, 90, 180]  # Forward return horizons
ZSCORE_THRESHOLDS = [-1.0, -1.5, -2.0, -2.5]  # Test these z-thresholds


def get_data():
    """Pull BTC dominance, alt mcap, BTC price, ETH/BTC from DB."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # BTC dominance
    cur.execute("""
        SELECT b.timestamp::date as dt,
               b.market_cap_usd / t.total_mcap_usd as dom
        FROM marketcap_daily b
        JOIN total_marketcap_daily t ON b.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
        ORDER BY b.timestamp
    """)
    dom_df = pd.DataFrame(cur.fetchall(), columns=["date", "dom"])
    dom_df["date"] = pd.to_datetime(dom_df["date"])
    dom_df = dom_df.set_index("date").sort_index()
    dom_df["dom"] = dom_df["dom"].astype(float) * 100  # to percentage

    # Alt mcap (total - BTC)
    cur.execute("""
        SELECT t.timestamp::date as dt,
               t.total_mcap_usd - b.market_cap_usd as alt_mcap
        FROM total_marketcap_daily t
        JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
        ORDER BY t.timestamp
    """)
    alt_df = pd.DataFrame(cur.fetchall(), columns=["date", "alt_mcap"])
    alt_df["date"] = pd.to_datetime(alt_df["date"])
    alt_df = alt_df.set_index("date").sort_index()
    alt_df["alt_mcap"] = alt_df["alt_mcap"].astype(float)

    # BTC price
    cur.execute("""
        SELECT timestamp::date as dt, price_usd
        FROM price_daily WHERE symbol = 'BTC' AND price_usd > 0
        ORDER BY timestamp
    """)
    btc_df = pd.DataFrame(cur.fetchall(), columns=["date", "btc"])
    btc_df["date"] = pd.to_datetime(btc_df["date"])
    btc_df = btc_df.set_index("date").sort_index()
    btc_df["btc"] = btc_df["btc"].astype(float)

    # ETH price
    cur.execute("""
        SELECT timestamp::date as dt, price_usd
        FROM price_daily WHERE symbol = 'ETH' AND price_usd > 0
        ORDER BY timestamp
    """)
    eth_df = pd.DataFrame(cur.fetchall(), columns=["date", "eth"])
    eth_df["date"] = pd.to_datetime(eth_df["date"])
    eth_df = eth_df.set_index("date").sort_index()
    eth_df["eth"] = eth_df["eth"].astype(float)

    conn.close()

    # Merge all
    df = dom_df.join(alt_df, how="inner").join(btc_df, how="inner").join(eth_df, how="left")
    df = df.dropna(subset=["dom", "alt_mcap", "btc"])

    return df


def compute_signals(df):
    """Compute rolling dominance change and z-score it."""

    # Rolling 30d change in dominance (in pp)
    df["dom_30d_change"] = df["dom"] - df["dom"].shift(LOOKBACK_DAYS)

    # Z-score against trailing 1-year distribution
    df["dom_change_mean"] = df["dom_30d_change"].rolling(ZSCORE_WINDOW, min_periods=180).mean()
    df["dom_change_std"] = df["dom_30d_change"].rolling(ZSCORE_WINDOW, min_periods=180).std()
    df["dom_change_zscore"] = (df["dom_30d_change"] - df["dom_change_mean"]) / df["dom_change_std"]

    # Forward returns for alt mcap
    for w in FORWARD_WINDOWS:
        df[f"alt_fwd_{w}d"] = df["alt_mcap"].shift(-w) / df["alt_mcap"] - 1
        df[f"btc_fwd_{w}d"] = df["btc"].shift(-w) / df["btc"] - 1
        df[f"alt_vs_btc_fwd_{w}d"] = df[f"alt_fwd_{w}d"] - df[f"btc_fwd_{w}d"]

    return df


def extract_signals(df, threshold, cooldown=SIGNAL_COOLDOWN):
    """Extract signal dates with cooldown dedup."""
    mask = df["dom_change_zscore"] < threshold
    signal_dates = df.index[mask].tolist()

    # Dedup with cooldown
    filtered = []
    for d in signal_dates:
        if not filtered or (d - filtered[-1]).days >= cooldown:
            filtered.append(d)

    return filtered


def compute_unconditional(df):
    """Compute unconditional (random date) forward returns for comparison."""
    stats = {}
    for w in FORWARD_WINDOWS:
        col = f"alt_fwd_{w}d"
        valid = df[col].dropna()
        stats[w] = {
            "mean": valid.mean() * 100,
            "median": valid.median() * 100,
            "std": valid.std() * 100,
            "win_rate": (valid > 0).mean() * 100,
            "n": len(valid),
        }
    return stats


def analyze_signals(df, signals, label):
    """Analyze forward returns after signal dates."""
    if not signals:
        print(f"\n  {label}: NO SIGNALS")
        return

    print(f"\n  {label}")
    print(f"  {'─' * 60}")
    print(f"  Signals: {len(signals)}")
    print(f"  Period: {signals[0].strftime('%Y-%m-%d')} to {signals[-1].strftime('%Y-%m-%d')}")
    print()

    # Show each signal with context
    print(f"  {'Date':>12s} {'Dom':>6s} {'30d Δ':>7s} {'Z':>6s} {'Alt 30d':>8s} {'Alt 60d':>8s} {'Alt 90d':>8s} {'vs BTC 90d':>10s}")
    print(f"  {'─'*12} {'─'*6} {'─'*7} {'─'*6} {'─'*8} {'─'*8} {'─'*8} {'─'*10}")

    for d in signals:
        row = df.loc[d]
        dom_val = row["dom"]
        change = row["dom_30d_change"]
        z = row["dom_change_zscore"]

        parts = [f"  {d.strftime('%Y-%m-%d'):>12s}", f"{dom_val:6.1f}", f"{change:+7.2f}", f"{z:+6.2f}"]

        for w in [30, 60, 90]:
            val = row.get(f"alt_fwd_{w}d")
            if pd.notna(val):
                parts.append(f"{val*100:+8.1f}%")
            else:
                parts.append(f"{'n/a':>8s}")

        vs_btc = row.get("alt_vs_btc_fwd_90d")
        if pd.notna(vs_btc):
            parts.append(f"{vs_btc*100:+10.1f}%")
        else:
            parts.append(f"{'n/a':>10s}")

        print(" ".join(parts))

    # Summary stats
    print(f"\n  Summary:")
    print(f"  {'Horizon':>10s} {'Avg':>8s} {'Median':>8s} {'StdDev':>8s} {'WinRate':>8s} {'vs Uncond':>10s}")
    print(f"  {'─'*10} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*10}")

    for w in FORWARD_WINDOWS:
        col = f"alt_fwd_{w}d"
        vals = [df.loc[d, col] for d in signals if d in df.index and pd.notna(df.loc[d, col])]
        if not vals:
            continue
        vals = np.array(vals) * 100

        avg = np.mean(vals)
        med = np.median(vals)
        std = np.std(vals)
        wr = (vals > 0).mean() * 100

        # Compare to unconditional
        uncond = df[col].dropna() * 100
        uncond_avg = uncond.mean()
        edge = avg - uncond_avg

        print(f"  {w:>8d}d {avg:+8.1f}% {med:+8.1f}% {std:8.1f}% {wr:7.0f}% {edge:+10.1f}pp")

    # Alt vs BTC spread
    print(f"\n  Alt vs BTC (relative outperformance after signal):")
    for w in FORWARD_WINDOWS:
        col = f"alt_vs_btc_fwd_{w}d"
        vals = [df.loc[d, col] for d in signals if d in df.index and pd.notna(df.loc[d, col])]
        if not vals: continue
        vals = np.array(vals) * 100
        avg = np.mean(vals)
        wr = (vals > 0).mean() * 100
        print(f"    {w:>3d}d: avg {avg:+.1f}pp, win rate {wr:.0f}%")


def main():
    print("━" * 70)
    print("  BTC DOMINANCE → RISK-ON EXPANSION BACKTEST")
    print("━" * 70)

    print("\n  Loading data...")
    df = get_data()
    print(f"  {len(df)} days: {df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}")

    print("\n  Computing signals...")
    df = compute_signals(df)

    # Distribution of 30d dominance changes
    valid_changes = df["dom_30d_change"].dropna()
    print(f"\n  30d dominance change distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(valid_changes, p):+.2f}pp")

    # Z-score distribution
    valid_z = df["dom_change_zscore"].dropna()
    print(f"\n  Z-score distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(valid_z, p):+.2f}")

    # Unconditional returns
    print(f"\n  Unconditional (random date) alt forward returns:")
    uncond = compute_unconditional(df)
    for w in FORWARD_WINDOWS:
        u = uncond[w]
        print(f"    {w:>3d}d: avg {u['mean']:+.1f}%, median {u['median']:+.1f}%, win rate {u['win_rate']:.0f}% (n={u['n']})")

    # Test each z-threshold
    for z_thresh in ZSCORE_THRESHOLDS:
        signals = extract_signals(df, z_thresh)
        analyze_signals(df, signals, f"Z < {z_thresh:.1f} (cooldown {SIGNAL_COOLDOWN}d)")

    print(f"\n{'━' * 70}")
    print(f"  INTERPRETATION")
    print(f"{'━' * 70}")
    print(f"  If signal forward returns are HIGHER than unconditional and")
    print(f"  win rates are HIGHER, the indicator has predictive value.")
    print(f"  Alt vs BTC > 0 means alts outperform BTC after the signal.")
    print(f"{'━' * 70}")


if __name__ == "__main__":
    main()
