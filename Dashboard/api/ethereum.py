"""
api/ethereum.py — Ethereum-specific endpoints
Same structure as bitcoin.py: MA overlay, MA gap, deviation, drawdown, mcap, ETH/BTC ratio
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn
import psycopg2.extras


def _fetch_eth_prices(cur, date_from, date_to):
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'ETH' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    rows = cur.fetchall()
    return [str(r['date']) for r in rows], [float(r['price_usd']) for r in rows]


def _sma(vals, window):
    return [None if i < window - 1 else sum(vals[i - window + 1:i + 1]) / window
            for i in range(len(vals))]


def handle_eth_ma(params):
    """ETH price with 50d, 200d, 200-week (1400d) MA overlaid."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=1410)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, prices = _fetch_eth_prices(cur, ext, date_to)
    conn.close()
    if not prices:
        return {"dates": [], "price": [], "ma50": [], "ma200": [], "ma200w": []}

    ma50  = _sma(prices, 50)
    ma200 = _sma(prices, 200)
    ma200w = _sma(prices, 1400)

    trimmed = [(d, p, m5, m2, mw) for d, p, m5, m2, mw
               in zip(dates, prices, ma50, ma200, ma200w) if d >= date_from]
    if not trimmed:
        return {"dates": [], "price": [], "ma50": [], "ma200": [], "ma200w": []}
    td, tp, t5, t2, tw = zip(*trimmed)
    return {"dates": list(td), "price": list(tp), "ma50": list(t5),
            "ma200": list(t2), "ma200w": list(tw)}


def handle_eth_ma_gap(params):
    """ETH 50d/200d MA gap over time."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, prices = _fetch_eth_prices(cur, ext, date_to)
    conn.close()
    if not prices:
        return {"dates": [], "gap": []}

    ma50  = _sma(prices, 50)
    ma200 = _sma(prices, 200)
    gap = [round((ma50[i] / ma200[i] - 1) * 100, 4) if ma50[i] and ma200[i] and ma200[i] > 0 else None
           for i in range(len(prices))]

    trimmed = [(d, g) for d, g in zip(dates, gap) if d >= date_from]
    if not trimmed: return {"dates": [], "gap": []}
    td, tg = zip(*trimmed)
    return {"dates": list(td), "gap": list(tg)}


def handle_eth_200d_dev(params):
    """ETH % deviation from 200d MA."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, prices = _fetch_eth_prices(cur, ext, date_to)
    conn.close()
    if not prices:
        return {"dates": [], "deviation": []}

    ma200 = _sma(prices, 200)
    dev = [round((prices[i] / ma200[i] - 1) * 100, 2) if ma200[i] and ma200[i] > 0 else None
           for i in range(len(prices))]

    trimmed = [(d, v) for d, v in zip(dates, dev) if d >= date_from]
    if not trimmed: return {"dates": [], "deviation": []}
    td, tv = zip(*trimmed)
    return {"dates": list(td), "deviation": list(tv)}


def handle_eth_drawdown(params):
    """ETH drawdown from running ATH."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily WHERE symbol = 'ETH' AND price_usd > 0 ORDER BY timestamp
    """)
    rows = cur.fetchall()
    conn.close()
    if not rows: return {"dates": [], "values": []}

    all_dates = [str(r['date']) for r in rows]
    prices = [float(r['price_usd']) for r in rows]
    rm = prices[0]
    dd = []
    for p in prices:
        if p > rm: rm = p
        dd.append(round((p / rm - 1) * 100, 4) if rm > 0 else 0)

    trimmed = [(d, v) for d, v in zip(all_dates, dd) if date_from <= d <= date_to]
    if not trimmed: return {"dates": [], "values": []}
    td, tv = zip(*trimmed)
    return {"dates": list(td), "values": list(tv)}


def handle_eth_mcap(params):
    """ETH market cap with milestone levels."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, market_cap_usd
        FROM marketcap_daily
        WHERE coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
          AND timestamp >= %s AND timestamp <= %s AND market_cap_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    rows = cur.fetchall()
    conn.close()
    if not rows: return {"dates": [], "mcap": [], "milestones": []}

    dates = [str(r['date']) for r in rows]
    mcaps = [float(r['market_cap_usd']) for r in rows]
    milestones = [{"label": "$50B", "value": 5e10}, {"label": "$100B", "value": 1e11},
                  {"label": "$200B", "value": 2e11}, {"label": "$500B", "value": 5e11}]
    return {"dates": dates, "mcap": mcaps, "milestones": milestones}


def handle_eth_btc_ratio(params):
    """ETH/BTC price ratio over time."""
    date_from = params.get("from", ["2017-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd FROM price_daily
        WHERE symbol = 'ETH' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    eth_rows = cur.fetchall()
    cur.execute("""
        SELECT timestamp::date as date, price_usd FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()
    conn.close()

    btc_map = {str(r['date']): float(r['price_usd']) for r in btc_rows}
    dates, values = [], []
    for r in eth_rows:
        d = str(r['date'])
        btc = btc_map.get(d)
        if btc and btc > 0:
            dates.append(d)
            values.append(round(float(r['price_usd']) / btc, 6))
    return {"dates": dates, "values": values}
