"""
api/macro.py — macro asset price endpoint
"""
from .shared import get_conn, rebase_series, macro_table, ts_cast
import psycopg2.extras


def handle_macro_price(params):
    symbols     = [s.strip() for s in params.get("symbols",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from", ["2024-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    granularity = params.get("granularity", ["daily"])[0]
    align       = params.get("align", ["own"])[0]
    if not symbols: return {"error": "no symbols"}

    tbl  = macro_table(granularity)
    cast = ts_cast(granularity)
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        SELECT ticker as symbol, {cast} as ts, close as price_usd
        FROM {tbl}
        WHERE ticker = ANY(%s) AND timestamp >= %s AND timestamp <= %s
        ORDER BY ticker, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    # Build per-symbol price maps
    price_maps = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in price_maps: price_maps[sym] = {}
        if row["price_usd"]:
            price_maps[sym][str(row["ts"])] = float(row["price_usd"])

    if not price_maps: return {}

    # For hourly: forward-fill gaps for market-hours tickers aligned to master index
    if granularity == "hourly" and len(price_maps) > 1:
        master_sym = max(price_maps, key=lambda s: len(price_maps[s]))
        master_ts  = sorted(price_maps[master_sym].keys())
        data = {}
        for sym in price_maps:
            prices, last_val = [], None
            for ts in master_ts:
                v = price_maps[sym].get(ts)
                if v is not None:
                    last_val = v
                elif last_val is not None:
                    v = last_val
                prices.append(v)
            data[sym] = {"dates": master_ts, "prices": prices}
    else:
        data = {}
        for sym, pmap in price_maps.items():
            sorted_ts = sorted(pmap.keys())
            data[sym] = {"dates": sorted_ts, "prices": [pmap[ts] for ts in sorted_ts]}

    if align == "common" and len(data) > 1:
        common_start = max(s["dates"][0] for s in data.values() if s["dates"])
        for sym in data:
            i = next((i for i,d in enumerate(data[sym]["dates"]) if d >= common_start), 0)
            data[sym]["dates"]  = data[sym]["dates"][i:]
            data[sym]["prices"] = data[sym]["prices"][i:]

    for sym in data:
        data[sym]["rebased"] = rebase_series(data[sym]["prices"])
    return data
