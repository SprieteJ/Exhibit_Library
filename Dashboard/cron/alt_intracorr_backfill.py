#!/usr/bin/env python3
"""
alt_intracorr_backfill.py
─────────────────────────
Precomputes 30d rolling avg pairwise correlation for top 10/25/50/100/250 alts.
Stores in alt_intracorr_daily.

Run:  DATABASE_URL="postgresql://..." python3 alt_intracorr_backfill.py
"""

import os, math
import pandas as pd
import psycopg2, psycopg2.extras
from datetime import datetime, timezone, timedelta
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]
WINDOW = 30
TIERS = {"top10": 10, "top25": 25, "top50": 50, "top100": 100, "top250": 250}
EXCLUDED = ('BTC','ETH','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def bulk_upsert(table, df, conflict_cols):
    if df.empty: return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cols = list(df.columns)
        cur.execute(f"CREATE TEMP TABLE _tmp (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cur.copy_from(buf, "_tmp", columns=cols, null="\\N")
        col_str = ", ".join(cols)
        conflict_str = ", ".join(conflict_cols)
        cur.execute(f"INSERT INTO {table} ({col_str}) SELECT {col_str} FROM _tmp ON CONFLICT ({conflict_str}) DO NOTHING")
        n = cur.rowcount
        conn.commit()
        return n
    except:
        conn.rollback()
        raise
    finally:
        conn.close()


def compute_intracorr():
    print("━" * 60)
    print("  ALT INTRACORRELATION BACKFILL")
    print("━" * 60)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("\nFetching top 250 alts by mcap...")
    cur.execute("""
        SELECT p.symbol
        FROM price_daily p
        JOIN (
            SELECT coingecko_id, market_cap_usd
            FROM marketcap_daily
            WHERE timestamp::date = (SELECT MAX(timestamp::date) FROM marketcap_daily WHERE timestamp <= NOW())
              AND market_cap_usd > 0
        ) m ON p.coingecko_id = m.coingecko_id
        WHERE p.symbol NOT IN %s
        GROUP BY p.symbol, m.market_cap_usd
        ORDER BY m.market_cap_usd DESC
        LIMIT 250
    """, (EXCLUDED,))
    ranked = [r['symbol'] for r in cur.fetchall()]
    print(f"  Found {len(ranked)} alts")

    if len(ranked) < 10:
        conn.close()
        return

    print("Fetching price history...")
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s) AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (ranked,))
    rows = cur.fetchall()
    conn.close()
    print(f"  {len(rows)} rows loaded")

    # Build returns
    prices = {}
    for r in rows:
        sym = r['symbol']
        if sym not in prices: prices[sym] = {}
        prices[sym][str(r['date'])] = float(r['price_usd'])

    returns = {}
    for sym in ranked:
        if sym not in prices: continue
        sd = sorted(prices[sym].keys())
        rets = {}
        for i in range(1, len(sd)):
            prev, curr = prices[sym].get(sd[i-1]), prices[sym].get(sd[i])
            if prev and curr and prev > 0:
                rets[sd[i]] = curr / prev - 1
        if len(rets) > WINDOW:
            returns[sym] = rets

    print(f"  {len(returns)} alts with return history")

    all_dates = sorted(set(d for rets in returns.values() for d in rets))

    # Check existing data
    conn2 = get_conn()
    cur2 = conn2.cursor()
    cur2.execute("SELECT MAX(timestamp) FROM alt_intracorr_daily")
    last = cur2.fetchone()[0]
    conn2.close()

    if last:
        start = (last.date() + timedelta(days=1)).isoformat()
        all_dates = [d for d in all_dates if d >= start]
        print(f"  Resuming from {start}")
    else:
        all_dates = all_dates[WINDOW:]
        print(f"  Full backfill from {all_dates[0] if all_dates else 'N/A'}")

    if not all_dates:
        print("  Up to date"); return

    print(f"  Computing {len(all_dates)} dates x {len(TIERS)} tiers...")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    full_dates = sorted(set(d for rets in returns.values() for d in rets))
    results = []

    for di, target in enumerate(all_dates):
        if di % 50 == 0:
            print(f"    {di}/{len(all_dates)} ({target})", end="\r")

        try:
            idx = full_dates.index(target)
        except ValueError:
            continue
        if idx < WINDOW: continue
        win = full_dates[idx - WINDOW + 1:idx + 1]

        for tier_name, tier_n in TIERS.items():
            syms = ranked[:min(tier_n, len(ranked))]
            vecs = {}
            for sym in syms:
                if sym not in returns: continue
                vec = [returns[sym].get(d) for d in win]
                if sum(1 for v in vec if v is not None) >= WINDOW // 2:
                    vecs[sym] = vec

            if len(vecs) < 3: continue

            keys = list(vecs.keys())
            corrs = []
            for i in range(len(keys)):
                for j in range(i+1, len(keys)):
                    pairs = [(a,b) for a,b in zip(vecs[keys[i]], vecs[keys[j]]) if a is not None and b is not None]
                    if len(pairs) < WINDOW // 3: continue
                    am = sum(p[0] for p in pairs)/len(pairs)
                    bm = sum(p[1] for p in pairs)/len(pairs)
                    num = sum((p[0]-am)*(p[1]-bm) for p in pairs)
                    da = math.sqrt(sum((p[0]-am)**2 for p in pairs))
                    db = math.sqrt(sum((p[1]-bm)**2 for p in pairs))
                    if da > 0 and db > 0:
                        corrs.append(num/(da*db))

            if corrs:
                results.append({"timestamp": target, "tier": tier_name,
                    "avg_corr": round(sum(corrs)/len(corrs), 6),
                    "source": "computed", "ingested_at": now_utc})

    print(f"\n  {len(results)} data points computed")

    if results:
        df = pd.DataFrame(results)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        n = bulk_upsert("alt_intracorr_daily", df, ["timestamp", "tier"])
        print(f"  +{n} rows inserted")

    print("\n" + "━" * 60)
    print("  DONE")
    print("━" * 60)


if __name__ == "__main__":
    compute_intracorr()
