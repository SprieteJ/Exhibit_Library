"""
api/alt_market.py — Altcoin market-wide endpoints
Altcoin mcap = total crypto mcap - BTC mcap - ETH mcap
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rolling_corr
import psycopg2.extras


def _sma(vals, window):
    return [None if i < window - 1 else sum(vals[i - window + 1:i + 1]) / window
            for i in range(len(vals))]


def _fetch_mcap_components(cur, date_from, date_to):
    """Returns aligned dates, btc_mcap, eth_mcap, total_mcap, alt_mcap arrays."""
    cur.execute("""
        SELECT b.timestamp::date as date,
               b.market_cap_usd as btc_mcap,
               e.market_cap_usd as eth_mcap,
               t.total_mcap_usd as total_mcap
        FROM marketcap_daily b
        JOIN marketcap_daily e ON b.timestamp::date = e.timestamp::date
        JOIN total_marketcap_daily t ON b.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
          AND b.timestamp >= %s AND b.timestamp <= %s
          AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
        ORDER BY b.timestamp::date
    """, (date_from, date_to))
    rows = cur.fetchall()
    if not rows:
        return [], [], [], [], []

    dates     = [str(r['date']) for r in rows]
    btc_mcap  = [float(r['btc_mcap']) for r in rows]
    eth_mcap  = [float(r['eth_mcap']) for r in rows]
    total_mcap = [float(r['total_mcap']) for r in rows]
    alt_mcap  = [t - b - e for t, b, e in zip(total_mcap, btc_mcap, eth_mcap)]
    return dates, btc_mcap, eth_mcap, total_mcap, alt_mcap


def handle_alt_mcap(params):
    """Altcoin mcap (ex-BTC, ex-ETH) with 50d/200d MA."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, _, _, _, alt_mcap = _fetch_mcap_components(cur, ext, date_to)
    conn.close()
    if not alt_mcap:
        return {"dates": [], "mcap": [], "ma50": [], "ma200": []}

    ma50  = _sma(alt_mcap, 50)
    ma200 = _sma(alt_mcap, 200)

    trimmed = [(d, m, m5, m2) for d, m, m5, m2 in zip(dates, alt_mcap, ma50, ma200) if d >= date_from]
    if not trimmed: return {"dates": [], "mcap": [], "ma50": [], "ma200": []}
    td, tm, t5, t2 = zip(*trimmed)
    return {"dates": list(td), "mcap": list(tm), "ma50": list(t5), "ma200": list(t2)}


def handle_alt_mcap_gap(params):
    """50d/200d MA gap on altcoin mcap."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, _, _, _, alt_mcap = _fetch_mcap_components(cur, ext, date_to)
    conn.close()
    if not alt_mcap: return {"dates": [], "gap": []}

    ma50  = _sma(alt_mcap, 50)
    ma200 = _sma(alt_mcap, 200)
    gap = [round((ma50[i] / ma200[i] - 1) * 100, 4) if ma50[i] and ma200[i] and ma200[i] > 0 else None
           for i in range(len(alt_mcap))]

    trimmed = [(d, g) for d, g in zip(dates, gap) if d >= date_from]
    if not trimmed: return {"dates": [], "gap": []}
    td, tg = zip(*trimmed)
    return {"dates": list(td), "gap": list(tg)}


def handle_alt_mcap_dev(params):
    """% deviation of altcoin mcap from its 200d MA."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, _, _, _, alt_mcap = _fetch_mcap_components(cur, ext, date_to)
    conn.close()
    if not alt_mcap: return {"dates": [], "deviation": []}

    ma200 = _sma(alt_mcap, 200)
    dev = [round((alt_mcap[i] / ma200[i] - 1) * 100, 2) if ma200[i] and ma200[i] > 0 else None
           for i in range(len(alt_mcap))]

    trimmed = [(d, v) for d, v in zip(dates, dev) if d >= date_from]
    if not trimmed: return {"dates": [], "deviation": []}
    td, tv = zip(*trimmed)
    return {"dates": list(td), "deviation": list(tv)}


def handle_dominance_shares(params):
    """BTC, ETH, Altcoin share of total mcap — 3 lines."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, btc_mcap, eth_mcap, total_mcap, alt_mcap = _fetch_mcap_components(cur, date_from, date_to)
    conn.close()
    if not dates: return {"dates": [], "btc_pct": [], "eth_pct": [], "alt_pct": []}

    btc_pct = [round(b / t * 100, 2) if t > 0 else None for b, t in zip(btc_mcap, total_mcap)]
    eth_pct = [round(e / t * 100, 2) if t > 0 else None for e, t in zip(eth_mcap, total_mcap)]
    alt_pct = [round(a / t * 100, 2) if t > 0 else None for a, t in zip(alt_mcap, total_mcap)]

    return {"dates": dates, "btc_pct": btc_pct, "eth_pct": eth_pct, "alt_pct": alt_pct}


def handle_alt_relative_share(params):
    """Altcoin mcap as % of: total mcap, BTC mcap, ETH mcap — 3 lines."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, btc_mcap, eth_mcap, total_mcap, alt_mcap = _fetch_mcap_components(cur, date_from, date_to)
    conn.close()
    if not dates: return {"dates": [], "vs_total": [], "vs_btc": [], "vs_eth": []}

    vs_total = [round(a / t * 100, 2) if t > 0 else None for a, t in zip(alt_mcap, total_mcap)]
    vs_btc   = [round(a / b * 100, 2) if b > 0 else None for a, b in zip(alt_mcap, btc_mcap)]
    vs_eth   = [round(a / e * 100, 2) if e > 0 else None for a, e in zip(alt_mcap, eth_mcap)]

    return {"dates": dates, "vs_total": vs_total, "vs_btc": vs_btc, "vs_eth": vs_eth}


def handle_btc_alt_ratio(params):
    """BTC mcap / Altcoin mcap ratio over time."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, btc_mcap, _, _, alt_mcap = _fetch_mcap_components(cur, date_from, date_to)
    conn.close()
    if not dates: return {"dates": [], "ratio": []}

    ratio = [round(b / a, 4) if a > 0 else None for b, a in zip(btc_mcap, alt_mcap)]
    return {"dates": dates, "ratio": ratio}


def handle_alt_intracorr(params):
    """Rolling 30d pairwise correlation within top-N altcoins by mcap.
    Returns separate series for top 10, 25, 50, 100, 250."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get all alts ranked by mcap
    cur.execute("""
        SELECT p.symbol, m.market_cap_usd
        FROM price_daily p
        JOIN (
            SELECT coingecko_id, market_cap_usd
            FROM marketcap_daily
            WHERE timestamp::date = (SELECT MAX(timestamp::date) FROM marketcap_daily WHERE timestamp <= NOW())
              AND market_cap_usd > 0
        ) m ON p.coingecko_id = m.coingecko_id
        WHERE p.symbol NOT IN ('BTC','ETH','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')
        GROUP BY p.symbol, m.market_cap_usd
        ORDER BY m.market_cap_usd DESC
        LIMIT 250
    """)
    ranked = [r['symbol'] for r in cur.fetchall()]

    if len(ranked) < 10:
        conn.close()
        return {"dates": [], "top10": [], "top25": [], "top50": [], "top100": [], "top250": []}

    # Fetch prices for all 250
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s) AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (ranked, ext, date_to))
    rows = cur.fetchall()
    conn.close()

    # Build price map
    prices = {}
    for r in rows:
        sym = r['symbol']
        if sym not in prices: prices[sym] = {}
        prices[sym][str(r['date'])] = float(r['price_usd'])

    all_dates = sorted(set(d for s in prices.values() for d in s))
    all_dates = [d for d in all_dates if d >= date_from]

    def compute_avg_corr(syms, dates_list):
        """Compute average pairwise rolling correlation for a set of symbols."""
        # Build return series
        ret_series = {}
        for sym in syms:
            if sym not in prices: continue
            sym_dates = sorted(prices[sym].keys())
            rets = {}
            for i in range(1, len(sym_dates)):
                d = sym_dates[i]
                prev = prices[sym].get(sym_dates[i-1])
                curr = prices[sym].get(d)
                if prev and curr and prev > 0:
                    rets[d] = curr / prev - 1
            if len(rets) > window:
                ret_series[sym] = rets

        if len(ret_series) < 3:
            return [None] * len(dates_list)

        # For each date, compute avg pairwise correlation over the window
        sym_list = list(ret_series.keys())
        result = []
        for d in dates_list:
            # Find window dates ending at d
            d_idx = all_dates.index(d) if d in all_dates else -1
            if d_idx < window: 
                result.append(None); continue

            win_dates = all_dates[d_idx - window + 1:d_idx + 1]
            
            # Get return vectors for this window
            vectors = {}
            for sym in sym_list:
                vec = [ret_series[sym].get(wd) for wd in win_dates]
                if sum(1 for v in vec if v is not None) >= window // 2:
                    vectors[sym] = vec

            if len(vectors) < 3:
                result.append(None); continue

            # Compute pairwise correlations
            syms_v = list(vectors.keys())
            corrs = []
            for i in range(len(syms_v)):
                for j in range(i + 1, len(syms_v)):
                    va = vectors[syms_v[i]]
                    vb = vectors[syms_v[j]]
                    pairs = [(a, b) for a, b in zip(va, vb) if a is not None and b is not None]
                    if len(pairs) < window // 3: continue
                    am = sum(p[0] for p in pairs) / len(pairs)
                    bm = sum(p[1] for p in pairs) / len(pairs)
                    num = sum((p[0]-am)*(p[1]-bm) for p in pairs)
                    da = math.sqrt(sum((p[0]-am)**2 for p in pairs))
                    db = math.sqrt(sum((p[1]-bm)**2 for p in pairs))
                    if da > 0 and db > 0:
                        corrs.append(num / (da * db))

            if corrs:
                result.append(round(sum(corrs) / len(corrs), 4))
            else:
                result.append(None)

        return result

    tiers = {"top10": 10, "top25": 25, "top50": 50, "top100": 100, "top250": 250}
    output = {"dates": all_dates}
    for key, n in tiers.items():
        syms = ranked[:min(n, len(ranked))]
        if len(syms) >= 3:
            output[key] = compute_avg_corr(syms, all_dates)
        else:
            output[key] = [None] * len(all_dates)

    return output
