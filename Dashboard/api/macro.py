"""
api/macro.py — macro asset price endpoint
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rebase_series, macro_table, ts_cast, macro_crypto_comparison, forward_fill, rolling_corr, macro_crypto_comparison, forward_fill, rolling_corr, macro_crypto_comparison, forward_fill, rolling_corr, rolling_corr, SECTORS, fetch_sector_index
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
        # Forward-fill weekend/holiday gaps for daily data
        # Use a master date list from the symbol with most data points
        master_sym = max(price_maps, key=lambda s: len(price_maps[s]))
        master_ts = sorted(price_maps[master_sym].keys())
        data = {}
        for sym, pmap in price_maps.items():
            prices, last_val = [], None
            for ts in master_ts:
                v = pmap.get(ts)
                if v is not None:
                    last_val = v
                elif last_val is not None:
                    v = last_val
                prices.append(v)
            data[sym] = {"dates": master_ts, "prices": prices}

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
    """DXY + BTC daily + rolling 30d correlation. Forward-filled + warmup."""
    date_from = params.get("from",   ["2022-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, dxy_vals, btc_vals, corr = macro_crypto_comparison(cur, 'DX-Y.NYB', 'BTC', date_from, date_to, window)
    conn.close()
    return {"dates": dates, "dxy": dxy_vals, "btc": btc_vals, "correlation": corr}
def handle_macro_igv_btc(params):
    """IGV (US Software ETF) + BTC daily + rolling 30d correlation. Forward-filled + warmup."""
    date_from = params.get("from",   ["2022-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, igv_vals, btc_vals, corr = macro_crypto_comparison(cur, 'IGV', 'BTC', date_from, date_to, window)
    conn.close()
    return {"dates": dates, "igv": igv_vals, "btc": btc_vals, "correlation": corr}
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


def handle_macro_sharpe(params):
    """Rolling Sharpe ratio for BTC, ETH, altcoin mcap, and macro assets.
    Sharpe = annualised_return / annualised_vol (risk-free = 0).
    Windows: 30, 90, 180, 365, 730, 1460 days."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["180"])[0])

    import math
    from datetime import timedelta

    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        ext = "2015-01-01"

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch crypto prices
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol IN ('BTC', 'ETH') AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (ext, date_to))
    crypto_rows = cur.fetchall()

    # Fetch macro prices
    macro_tickers = ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BNO']
    cur.execute("""
        SELECT ticker as symbol, timestamp::date as date, close as price_usd
        FROM macro_daily
        WHERE ticker = ANY(%s) AND timestamp >= %s AND timestamp <= %s AND close > 0
        ORDER BY ticker, timestamp
    """, (macro_tickers, ext, date_to))
    macro_rows = cur.fetchall()

    # Fetch alt mcap
    cur.execute("""
        SELECT t.timestamp::date as date,
               t.total_mcap_usd - b.market_cap_usd - e.market_cap_usd as price_usd
        FROM total_marketcap_daily t
        JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
        JOIN marketcap_daily e ON e.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
          AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
          AND t.timestamp >= %s AND t.timestamp <= %s
        ORDER BY t.timestamp
    """, (ext, date_to))
    alt_rows = [{"symbol": "ALTS", "date": r["date"], "price_usd": float(r["price_usd"])} for r in cur.fetchall()]

    conn.close()

    # Build price maps
    all_rows = crypto_rows + macro_rows + alt_rows
    prices = {}
    for r in all_rows:
        sym = r["symbol"]
        if sym not in prices: prices[sym] = {}
        prices[sym][str(r["date"])] = float(r["price_usd"])

    # Compute rolling Sharpe for each asset
    labels = {"BTC": "Bitcoin", "ETH": "Ethereum", "ALTS": "Altcoins",
              "SPY": "S&P 500", "QQQ": "Nasdaq", "IWM": "Russell 2000",
              "TLT": "US Treasuries", "GLD": "Gold", "BNO": "Brent Oil"}

    result = {}
    for sym, pmap in prices.items():
        sorted_dates = sorted(pmap.keys())
        vals = [pmap[d] for d in sorted_dates]

        # Daily log returns
        log_rets = [None] + [math.log(vals[i] / vals[i-1]) if vals[i-1] > 0 else None
                             for i in range(1, len(vals))]

        # Rolling Sharpe
        sharpe_dates = []
        sharpe_vals = []
        for i in range(window, len(log_rets)):
            wr = [r for r in log_rets[i - window + 1:i + 1] if r is not None]
            if len(wr) < window // 2:
                continue
            d = sorted_dates[i]
            if d < date_from:
                continue
            mean_ret = sum(wr) / len(wr)
            std_ret = math.sqrt(sum((r - mean_ret)**2 for r in wr) / len(wr))
            if std_ret > 0:
                sharpe = (mean_ret / std_ret) * math.sqrt(365)
            else:
                sharpe = 0
            sharpe_dates.append(d)
            sharpe_vals.append(round(sharpe, 4))

        if sharpe_dates:
            result[sym] = {
                "label": labels.get(sym, sym),
                "dates": sharpe_dates,
                "sharpe": sharpe_vals,
                "current": sharpe_vals[-1] if sharpe_vals else None,
            }

    return {"window": window, "assets": result}


def handle_macro_btc_corr(params):
    """Rolling correlation of macro assets vs BTC using log-normalized daily returns."""
    symbols     = [s.strip() for s in params.get("symbols", ["QQQ,GLD"])[0].split(",") if s.strip()]
    date_from   = params.get("from", ["2023-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    window      = int(params.get("window", ["30"])[0])

    from datetime import timedelta
    import math

    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window + 30)).strftime("%Y-%m-%d")
    except:
        ext = "2020-01-01"

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch BTC daily prices
    cur.execute("""
        SELECT timestamp::date as date, price_usd FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (ext, date_to))
    btc_rows = cur.fetchall()
    btc_prices = {str(r["date"]): float(r["price_usd"]) for r in btc_rows}

    # Fetch macro daily prices
    cur.execute("""
        SELECT ticker as symbol, timestamp::date as date, close as price_usd
        FROM macro_daily
        WHERE ticker = ANY(%s) AND timestamp >= %s AND timestamp <= %s AND close > 0
        ORDER BY ticker, timestamp
    """, (symbols, ext, date_to))
    macro_rows = cur.fetchall()
    conn.close()

    # Build price maps
    macro_prices = {}
    for r in macro_rows:
        sym = r["symbol"]
        if sym not in macro_prices: macro_prices[sym] = {}
        macro_prices[sym][str(r["date"])] = float(r["price_usd"])

    if not btc_prices or not macro_prices:
        return {"window": window, "assets": {}}

    # Common date axis (forward-fill weekends for macro)
    all_dates = sorted(set(btc_prices.keys()))

    # Compute BTC log returns
    btc_rets = {}
    prev = None
    for d in all_dates:
        p = btc_prices.get(d)
        if p and prev and prev > 0:
            btc_rets[d] = math.log(p / prev)
        prev = p if p else prev

    labels = {"SPY": "S&P 500", "QQQ": "Nasdaq", "IWM": "Russell 2000",
              "TLT": "US Treasuries", "GLD": "Gold", "BNO": "Brent Oil",
              "DX-Y.NYB": "US Dollar (DXY)", "^VIX": "VIX", "^TNX": "10Y Yield",
              "IBIT": "BTC ETF (IBIT)", "MSTR": "MicroStrategy", "COIN": "Coinbase"}

    result = {}
    for sym, pmap in macro_prices.items():
        # Forward-fill macro prices across all dates
        filled = {}
        last_val = None
        for d in all_dates:
            v = pmap.get(d)
            if v is not None:
                last_val = v
            elif last_val is not None:
                v = last_val
            if v: filled[d] = v

        # Compute macro log returns
        macro_rets = {}
        prev = None
        for d in all_dates:
            p = filled.get(d)
            if p and prev and prev > 0:
                macro_rets[d] = math.log(p / prev)
            prev = p if p else prev

        # Rolling correlation
        corr_dates = []
        corr_vals = []
        for i in range(len(all_dates)):
            d = all_dates[i]
            if d < date_from: continue

            # Get window of paired returns
            window_dates = all_dates[max(0, i - window + 1):i + 1]
            pairs = [(btc_rets[wd], macro_rets[wd])
                     for wd in window_dates
                     if wd in btc_rets and wd in macro_rets]

            if len(pairs) < window // 2: continue

            bx = [p[0] for p in pairs]
            mx = [p[1] for p in pairs]
            n = len(pairs)
            bm = sum(bx) / n
            mm = sum(mx) / n
            num = sum((b - bm) * (m - mm) for b, m in pairs)
            d_b = math.sqrt(sum((b - bm)**2 for b in bx))
            d_m = math.sqrt(sum((m - mm)**2 for m in mx))

            if d_b > 0 and d_m > 0:
                corr = num / (d_b * d_m)
            else:
                corr = 0

            corr_dates.append(d)
            corr_vals.append(round(corr, 4))

        if corr_dates:
            result[sym] = {
                "label": labels.get(sym, sym),
                "dates": corr_dates,
                "corr": corr_vals,
                "current": corr_vals[-1] if corr_vals else None,
            }

    return {"window": window, "assets": result}
