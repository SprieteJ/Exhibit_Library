"""
api/macro.py — macro asset price endpoint
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rebase_series, macro_table, ts_cast, rolling_corr, SECTORS, fetch_sector_index
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


def handle_macro_matrix(params):
    """Correlation heatmap: macro tickers vs crypto sector EW indices, last value of rolling window."""
    date_from = params.get("from",   ["2024-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    MACRO_SUBSET = ["SPY", "QQQ", "GLD", "DX-Y.NYB", "^VIX", "^TNX", "TLT", "BNO"]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch macro prices
    cur.execute("""
        SELECT ticker, timestamp::date as date, close
        FROM macro_daily
        WHERE ticker = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND close > 0
        ORDER BY ticker, timestamp
    """, (MACRO_SUBSET, date_from, date_to))
    macro_rows = cur.fetchall()

    macro_prices = {}
    for row in macro_rows:
        t = row['ticker']
        if t not in macro_prices: macro_prices[t] = {}
        macro_prices[t][str(row['date'])] = float(row['close'])

    present_macro = [t for t in MACRO_SUBSET if t in macro_prices and len(macro_prices[t]) >= window]

    # Fetch sector EW indices
    sector_indices = {}
    for sector, cg_ids in SECTORS.items():
        if not cg_ids: continue
        index, dates = fetch_sector_index(cur, cg_ids, date_from, date_to, 'equal', 'daily')
        if dates:
            sector_indices[sector] = index

    conn.close()

    if not present_macro or not sector_indices:
        return {"error": "insufficient data"}

    all_dates = sorted(set(
        d for prices in list(macro_prices.values()) + list(sector_indices.values()) for d in prices
    ))

    matrix = []
    for macro_t in present_macro:
        row_vals = []
        for sector in sector_indices:
            xa = [macro_prices[macro_t].get(d) for d in all_dates]
            xb = [sector_indices[sector].get(d) for d in all_dates]
            corr = rolling_corr(xa, xb, window)
            last = next((v for v in reversed(corr) if v is not None), None)
            row_vals.append(last)
        matrix.append(row_vals)

    return {
        "macro_tickers": present_macro,
        "crypto_sectors": list(sector_indices.keys()),
        "matrix": matrix,
    }


def handle_macro_dxy_btc(params):
    """DXY + BTC daily + rolling 30d correlation."""
    date_from = params.get("from",   ["2022-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, close
        FROM macro_daily
        WHERE ticker = 'DX-Y.NYB'
          AND timestamp >= %s AND timestamp <= %s
          AND close > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    dxy_rows = cur.fetchall()

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()
    conn.close()

    dxy_map = {str(r['date']): float(r['close']) for r in dxy_rows}
    btc_map = {str(r['date']): float(r['price_usd']) for r in btc_rows}

    all_dates = sorted(set(list(dxy_map.keys()) + list(btc_map.keys())))
    dxy_vals = [dxy_map.get(d) for d in all_dates]
    btc_vals = [btc_map.get(d) for d in all_dates]
    corr     = rolling_corr(dxy_vals, btc_vals, window)

    return {"dates": all_dates, "dxy": dxy_vals, "btc": btc_vals, "correlation": corr}


def handle_macro_risk(params):
    """Composite risk-on/off score: VIX + DXY (+ HYG/LQD if available)."""
    date_from = params.get("from", ["2022-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    # Need trailing 365d for normalization
    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    TICKERS = ["^VIX", "DX-Y.NYB", "HYG", "LQD"]
    cur.execute("""
        SELECT ticker, timestamp::date as date, close
        FROM macro_daily
        WHERE ticker = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND close > 0
        ORDER BY ticker, timestamp
    """, (TICKERS, dt_from_ext, date_to))
    macro_rows = cur.fetchall()

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()
    conn.close()

    ticker_data = {}
    for row in macro_rows:
        t = row['ticker']
        if t not in ticker_data: ticker_data[t] = {}
        ticker_data[t][str(row['date'])] = float(row['close'])

    btc_map = {str(r['date']): float(r['price_usd']) for r in btc_rows}

    # Build HYG/LQD ratio if both available
    have_hyglyq = 'HYG' in ticker_data and 'LQD' in ticker_data
    all_dates = sorted(set(list(ticker_data.get('^VIX', {}).keys()) + list(ticker_data.get('DX-Y.NYB', {}).keys())))

    def rolling_min_max(series, keys, trail=365):
        result = {}
        for i, d in enumerate(keys):
            v = series.get(d)
            if v is None:
                result[d] = None
                continue
            window_start = keys[max(0, i - trail)]
            window_vals  = [series.get(k) for k in keys[max(0, i - trail):i + 1] if series.get(k) is not None]
            if not window_vals:
                result[d] = None
                continue
            mn, mx = min(window_vals), max(window_vals)
            result[d] = (v - mn) / (mx - mn) if mx > mn else 0.5
        return result

    vix_norm = rolling_min_max(ticker_data.get('^VIX', {}), all_dates)
    dxy_norm = rolling_min_max(ticker_data.get('DX-Y.NYB', {}), all_dates)

    if have_hyglyq:
        ratio_map = {}
        for d in all_dates:
            h = ticker_data['HYG'].get(d)
            l = ticker_data['LQD'].get(d)
            if h and l and l > 0:
                ratio_map[d] = h / l
        hyglyq_norm = rolling_min_max(ratio_map, all_dates)
    else:
        hyglyq_norm = {}

    scores_all = {}
    for d in all_dates:
        components = []
        v = vix_norm.get(d)
        if v is not None:
            components.append(1 - v)  # high VIX = risk off → low score
        dx = dxy_norm.get(d)
        if dx is not None:
            components.append(1 - dx)  # high DXY = risk off
        if have_hyglyq:
            hl = hyglyq_norm.get(d)
            if hl is not None:
                components.append(hl)  # high HYG/LQD = risk on
        if components:
            scores_all[d] = round(sum(components) / len(components) * 100, 2)

    # Trim to date_from
    result_dates = [d for d in all_dates if d >= date_from]
    scores = [scores_all.get(d) for d in result_dates]
    btc    = [btc_map.get(d) for d in result_dates]

    return {"dates": result_dates, "score": scores, "btc": btc}


def handle_macro_real_yields(params):
    """10Y yield (^TNX) + BTC price."""
    date_from = params.get("from", ["2022-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, close
        FROM macro_daily
        WHERE ticker = '^TNX'
          AND timestamp >= %s AND timestamp <= %s
          AND close > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    tnx_rows = cur.fetchall()

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()
    conn.close()

    tnx_map = {str(r['date']): float(r['close']) for r in tnx_rows}
    btc_map = {str(r['date']): float(r['price_usd']) for r in btc_rows}

    all_dates = sorted(set(list(tnx_map.keys()) + list(btc_map.keys())))
    yield_10y = [tnx_map.get(d) for d in all_dates]
    btc_vals  = [btc_map.get(d) for d in all_dates]

    return {"dates": all_dates, "yield_10y": yield_10y, "btc": btc_vals}


def handle_macro_stablecoin(params):
    """Total market cap of Stablecoins sector over time."""
    date_from = params.get("from", ["2022-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    from api.shared import SECTORS
    stablecoin_ids = SECTORS.get("Stablecoins", [])
    if not stablecoin_ids:
        return {"dates": [], "values": []}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, SUM(market_cap_usd) as total_mcap
        FROM marketcap_daily
        WHERE coingecko_id = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND market_cap_usd > 0
        GROUP BY timestamp::date
        ORDER BY timestamp::date
    """, (stablecoin_ids, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    return {
        "dates":  [str(r['date']) for r in rows],
        "values": [float(r['total_mcap']) for r in rows],
    }
