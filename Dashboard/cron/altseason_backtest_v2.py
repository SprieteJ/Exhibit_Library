#!/usr/bin/env python3
"""
Altseason Indicator → Risk-On Expansion Backtest
────────────────────────────────────────────────
The altseason indicator = % of top N alts outperforming BTC over 90d.
Tests whether elevated altseason readings predict continued alt outperformance.

Tests:
1. Raw threshold: altseason > 50%, 60%, 70%, 75%
2. Altseason rising: 30d change in altseason indicator
3. Combined: altseason + dom falling
4. Combined: altseason + funding positive
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

    # Get top 50 alts by current market cap (excluding BTC, ETH, stables)
    cur.execute("""
        SELECT coingecko_id, symbol FROM asset_registry
        WHERE symbol NOT IN ('BTC', 'ETH', 'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'FDUSD', 'USDD', 'PYUSD')
          AND sector IS NOT NULL
        ORDER BY symbol
        
    """)
    alts = cur.fetchall()
    alt_ids = [r[0] for r in alts]

    # BTC prices
    cur.execute("""
        SELECT timestamp::date as dt, price_usd FROM price_daily
        WHERE symbol = 'BTC' AND price_usd > 0 ORDER BY timestamp
    """)
    btc_df = pd.DataFrame(cur.fetchall(), columns=["date", "btc"])
    btc_df["date"] = pd.to_datetime(btc_df["date"])
    btc_df = btc_df.set_index("date").sort_index()
    btc_df["btc"] = btc_df["btc"].astype(float)

    # Alt prices
    cur.execute("""
        SELECT timestamp::date as dt, coingecko_id, price_usd FROM price_daily
        WHERE coingecko_id = ANY(%s) AND price_usd > 0
        ORDER BY timestamp
    """, (alt_ids,))
    alt_prices = {}
    for r in cur.fetchall():
        cg_id = r[1]
        if cg_id not in alt_prices:
            alt_prices[cg_id] = {}
        alt_prices[cg_id][str(r[0])] = float(r[2])

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
    alt_mcap = pd.DataFrame(cur.fetchall(), columns=["date", "alt_mcap"])
    alt_mcap["date"] = pd.to_datetime(alt_mcap["date"])
    alt_mcap = alt_mcap.set_index("date").sort_index()
    alt_mcap["alt_mcap"] = alt_mcap["alt_mcap"].astype(float)

    conn.close()
    return btc_df, alt_prices, alt_mcap


def compute_altseason(btc_df, alt_prices, lookback=90):
    """Compute daily altseason indicator: % of alts outperforming BTC over lookback days."""
    dates = sorted(btc_df.index)
    btc_prices = btc_df["btc"].to_dict()

    results = {}
    for d in dates:
        d_str = str(d.date())
        d_back = str((d - timedelta(days=lookback)).date())

        btc_now = btc_prices.get(d)
        btc_then = btc_prices.get(d - timedelta(days=lookback))
        if not btc_now or not btc_then or btc_then <= 0:
            continue
        btc_ret = btc_now / btc_then - 1

        outperforming = 0
        total = 0
        for cg_id, prices in alt_prices.items():
            p_now = prices.get(d_str)
            p_then = prices.get(d_back)
            if p_now and p_then and p_then > 0:
                alt_ret = p_now / p_then - 1
                total += 1
                if alt_ret > btc_ret:
                    outperforming += 1

        if total >= 10:
            results[d] = outperforming / total * 100

    return pd.Series(results, name="altseason")


def main():
    print("━" * 78)
    print("  ALTSEASON INDICATOR → RISK-ON EXPANSION BACKTEST")
    print("━" * 78)

    print("\n  Loading data...")
    btc_df, alt_prices, alt_mcap = get_data()
    print(f"  BTC: {len(btc_df)} days, {len(alt_prices)} alts tracked")

    print("  Computing altseason indicator (90d lookback)...")
    altseason = compute_altseason(btc_df, alt_prices, lookback=90)
    print(f"  Altseason computed: {len(altseason)} days, {altseason.index[0].date()} to {altseason.index[-1].date()}")

    # Merge
    df = pd.DataFrame(altseason).join(alt_mcap, how="inner").join(btc_df, how="inner")
    df = df.dropna(subset=["altseason", "alt_mcap", "btc"])

    # 30d change in altseason
    df["alt_30d_change"] = df["altseason"] - df["altseason"].shift(30)

    # Forward returns
    for w in FWD_WINDOWS:
        df[f"alt_fwd_{w}"] = df["alt_mcap"].shift(-w) / df["alt_mcap"] - 1
        df[f"btc_fwd_{w}"] = df["btc"].shift(-w) / df["btc"] - 1
        df[f"spread_fwd_{w}"] = df[f"alt_fwd_{w}"] - df[f"btc_fwd_{w}"]

    # Distribution
    print(f"\n  Altseason indicator distribution:")
    for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
        print(f"    {p:3d}th pctl: {np.percentile(df['altseason'].dropna(), p):.1f}%")

    # Unconditional
    print(f"\n  ── UNCONDITIONAL baseline ──")
    for w in FWD_WINDOWS:
        v = df[f"alt_fwd_{w}"].dropna() * 100
        s = df[f"spread_fwd_{w}"].dropna() * 100
        print(f"    {w:>3d}d: alt avg {v.mean():+.1f}% WR {(v>0).mean()*100:.0f}% | spread {s.mean():+.1f}pp WR {(s>0).mean()*100:.0f}%")

    def dedup(dates):
        filtered = []
        for d in dates:
            if not filtered or (d - filtered[-1]).days >= COOLDOWN:
                filtered.append(d)
        return filtered

    def summarize(sigs, label):
        n = len(sigs)
        if n == 0:
            print(f"\n  {label}: NO SIGNALS\n")
            return
        print(f"\n  {label}")
        print(f"  {'─' * 70}")
        print(f"  Signals: {n} | {sigs[0].date()} to {sigs[-1].date()}")

        print(f"\n  {'Date':>12s} {'AltSzn':>7s} {'30dΔ':>7s} | {'Alt30d':>8s} {'Alt90d':>8s} {'vsB90d':>8s}")
        print(f"  {'─'*12} {'─'*7} {'─'*7}   {'─'*8} {'─'*8} {'─'*8}")
        for d in sigs:
            r = df.loc[d]
            a = r["altseason"]
            c = r["alt_30d_change"] if pd.notna(r["alt_30d_change"]) else 0
            parts = [f"  {d.date()!s:>12s}", f"{a:7.1f}%", f"{c:+7.1f}", "|"]
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

    # ── Test 1: Raw altseason level ──
    for thresh in [40, 50, 60, 70, 75]:
        mask = df["altseason"] > thresh
        sigs = dedup(df.index[mask].tolist())
        summarize(sigs, f"TEST 1: Altseason > {thresh}%")

    # ── Test 2: Altseason rising (30d change) ──
    for delta in [5, 10, 15, 20]:
        mask = df["alt_30d_change"] > delta
        sigs = dedup(df.index[mask.fillna(False)].tolist())
        summarize(sigs, f"TEST 2: Altseason 30d change > +{delta}pp")

    # ── Test 3: Altseason crossing above threshold (event) ──
    for thresh in [50, 60]:
        crossed = (df["altseason"] > thresh) & (df["altseason"].shift(1) <= thresh)
        sigs = dedup(df.index[crossed.fillna(False)].tolist())
        summarize(sigs, f"TEST 3: Altseason crossing above {thresh}% (event)")

    # ── Comparison ──
    print(f"\n{'━' * 78}")
    print(f"  COMPARISON TABLE (90d forward)")
    print(f"{'━' * 78}")
    print(f"\n  {'Filter':55s} {'#Sig':>5s} | {'Alt90d':>8s} {'WR':>5s} | {'Sprd90d':>9s} {'WR':>5s}")
    print(f"  {'─'*55} {'─'*5}   {'─'*8} {'─'*5}   {'─'*9} {'─'*5}")

    tests = [
        ("Altseason > 40%", df["altseason"] > 40),
        ("Altseason > 50%", df["altseason"] > 50),
        ("Altseason > 60%", df["altseason"] > 60),
        ("Altseason > 70%", df["altseason"] > 70),
        ("Altseason > 75%", df["altseason"] > 75),
        ("Altseason 30d change > +5pp", df["alt_30d_change"] > 5),
        ("Altseason 30d change > +10pp", df["alt_30d_change"] > 10),
        ("Altseason 30d change > +15pp", df["alt_30d_change"] > 15),
        ("Altseason crossing 50%", (df["altseason"] > 50) & (df["altseason"].shift(1) <= 50)),
        ("Altseason crossing 60%", (df["altseason"] > 60) & (df["altseason"].shift(1) <= 60)),
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
