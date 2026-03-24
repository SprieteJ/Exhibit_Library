"""
api/altcoins.py — altcoin price comparison + performance vs BTC scatter
"""
import math
from datetime import datetime, timedelta
from .shared import get_conn, rebase_series, price_table, ts_cast
import psycopg2.extras


def handle_price(params):
    symbols     = [s.strip().upper() for s in params.get("symbols",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from", ["2024-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    granularity = params.get("granularity", ["daily"])[0]
    align       = params.get("align", ["own"])[0]
    if not symbols: return {"error": "no symbols"}

    tbl  = price_table(granularity)
    cast = ts_cast(granularity)
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        SELECT symbol, {cast} as ts, price_usd
        FROM {tbl}
        WHERE symbol = ANY(%s) AND timestamp >= %s AND timestamp <= %s
        ORDER BY symbol, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    data = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in data: data[sym] = {"dates": [], "prices": []}
        data[sym]["dates"].append(str(row["ts"]))
        data[sym]["prices"].append(float(row["price_usd"]) if row["price_usd"] else None)

    if align == "common" and len(data) > 1:
        common_start = max(s["dates"][0] for s in data.values() if s["dates"])
        for sym in data:
            i = next((i for i,d in enumerate(data[sym]["dates"]) if d >= common_start), 0)
            data[sym]["dates"]  = data[sym]["dates"][i:]
            data[sym]["prices"] = data[sym]["prices"][i:]

    for sym in data:
        data[sym]["rebased"] = rebase_series(data[sym]["prices"])
    return data


def handle_alt_scatter(params):
    """
    For top N altcoins by mcap (ex BTC/ETH):
      y = % return vs BTC over window
      x = daily return volatility vs BTC (higher = more volatile than BTC)
    X-axis is reversed on frontend so high vol is on the left.
    """
    date_to = params.get("to",   ["2099-01-01"])[0]
    days    = int(params.get("days", ["7"])[0])
    topn    = int(params.get("topn", ["50"])[0])

    try:
        dt_to   = datetime.strptime(date_to, "%Y-%m-%d")
        dt_from = (dt_to - timedelta(days=days + 5)).strftime("%Y-%m-%d")
    except:
        dt_from = "2024-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Top N altcoins by most recent market cap
    cur.execute("""
        SELECT DISTINCT ON (p.symbol) p.symbol, m.market_cap_usd
        FROM price_daily p
        JOIN marketcap_daily m ON p.coingecko_id = m.coingecko_id
            AND p.timestamp::date = m.timestamp::date
        WHERE p.symbol NOT IN ('BTC','ETH')
          AND m.market_cap_usd > 0
        ORDER BY p.symbol, m.timestamp DESC
    """)
    all_assets = sorted(cur.fetchall(), key=lambda r: r["market_cap_usd"] or 0, reverse=True)[:topn]
    symbols    = [r["symbol"] for r in all_assets]

    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (["BTC"] + symbols, dt_from, date_to))
    rows = cur.fetchall()
    conn.close()

    prices = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in prices: prices[sym] = {}
        prices[sym][str(row["date"])] = float(row["price_usd"])

    if "BTC" not in prices or len(prices["BTC"]) < 2:
        return {"error": "insufficient BTC data", "points": []}

    btc_vals   = [prices["BTC"][d] for d in sorted(prices["BTC"])]
    btc_return = (btc_vals[-1] / btc_vals[0] - 1) * 100 if len(btc_vals) >= 2 else 0
    btc_rets   = [(btc_vals[i]/btc_vals[i-1]-1)*100 for i in range(1, len(btc_vals))]
    btc_mean   = sum(btc_rets)/len(btc_rets) if btc_rets else 0
    btc_vol    = math.sqrt(sum((r-btc_mean)**2 for r in btc_rets)/len(btc_rets)) if btc_rets else 1

    points = []
    for sym in symbols:
        if sym not in prices or len(prices[sym]) < 2: continue
        sym_vals   = [prices[sym][d] for d in sorted(prices[sym])]
        sym_return = (sym_vals[-1]/sym_vals[0]-1)*100
        sym_rets   = [(sym_vals[i]/sym_vals[i-1]-1)*100 for i in range(1, len(sym_vals))]
        sym_mean   = sum(sym_rets)/len(sym_rets) if sym_rets else 0
        sym_vol    = math.sqrt(sum((r-sym_mean)**2 for r in sym_rets)/len(sym_rets)) if sym_rets else 0

        points.append({
            "symbol": sym,
            "perf":   round(sym_return - btc_return, 2),
            "vol":    round(sym_vol - btc_vol, 2),
        })

    return {"points": points, "btc_return": round(btc_return, 2)}
