"""
api/alt_market.py — Altcoin market-wide endpoints
Altcoin mcap = total crypto mcap - BTC mcap - ETH mcap
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rolling_corr
import psycopg2.extras


def _slope(series, window=5):
    result = [None] * len(series)
    for i in range(window, len(series)):
        if series[i] is not None and series[i - window] is not None and series[i - window] != 0:
            result[i] = round((series[i] / series[i - window] - 1) * 100, 4)
    return result

def _inflections(slope_series):
    result = [False] * len(slope_series)
    for i in range(1, len(slope_series)):
        if slope_series[i] is not None and slope_series[i - 1] is not None:
            if (slope_series[i - 1] < 0 and slope_series[i] >= 0) or \
               (slope_series[i - 1] > 0 and slope_series[i] <= 0):
                result[i] = True
    return result


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
    gap_slope = _slope(list(tg))
    gap_infl  = _inflections(gap_slope)
    return {"dates": list(td), "gap": list(tg), "gap_slope": gap_slope, "gap_inflections": gap_infl}


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
    dev_slope = _slope(list(tv))
    dev_infl  = _inflections(dev_slope)
    return {"dates": list(td), "deviation": list(tv), "dev_slope": dev_slope, "dev_inflections": dev_infl}


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
    Reads from precomputed alt_intracorr_daily table."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, tier, avg_corr
        FROM alt_intracorr_daily
        WHERE timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "top10": [], "top25": [], "top50": [], "top100": [], "top250": []}

    tiers = {"top10": {}, "top25": {}, "top50": {}, "top100": {}, "top250": {}}
    all_dates = set()
    for r in rows:
        d = str(r['date'])
        tier = r['tier']
        all_dates.add(d)
        if tier in tiers:
            tiers[tier][d] = float(r['avg_corr']) if r['avg_corr'] is not None else None

    dates = sorted(all_dates)
    output = {"dates": dates}
    for tier_name in ["top10", "top25", "top50", "top100", "top250"]:
        output[tier_name] = [tiers[tier_name].get(d) for d in dates]

    return output
