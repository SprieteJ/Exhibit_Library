"""
api/altcoins.py — altcoin price comparison + performance vs BTC scatter
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rebase_series, price_table, ts_cast, rolling_corr, SECTORS, SECTOR_COLORS
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
    Top N altcoins by mcap (ex BTC/ETH):
      y = % return vs BTC over window
      x = daily return vol vs BTC vol
    """
    date_to = params.get("to",   ["2099-01-01"])[0]
    days    = int(params.get("days", ["7"])[0])
    topn    = int(params.get("topn", ["50"])[0])

    # Cap topn to avoid runaway queries
    topn = min(topn, 250)

    try:
        dt_to   = datetime.strptime(min(date_to, "2099-01-01"), "%Y-%m-%d")
        dt_from = (dt_to - timedelta(days=days + 5)).strftime("%Y-%m-%d")
        dt_to_s = dt_to.strftime("%Y-%m-%d")
    except:
        dt_from = "2024-01-01"
        dt_to_s = "2099-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Step 1: get top N symbols by most recent mcap using a tight date window
    cur.execute("""
        SELECT p.symbol
        FROM price_daily p
        JOIN (
            SELECT coingecko_id, market_cap_usd
            FROM marketcap_daily
            WHERE timestamp::date = (
                SELECT MAX(timestamp::date) FROM marketcap_daily
                WHERE timestamp <= NOW()
            )
            AND market_cap_usd > 0
        ) m ON p.coingecko_id = m.coingecko_id
        WHERE p.symbol NOT IN ('BTC','ETH','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')
        GROUP BY p.symbol, m.market_cap_usd
        ORDER BY m.market_cap_usd DESC
        LIMIT %s
    """, (topn,))
    symbols = [r["symbol"] for r in cur.fetchall()]

    if not symbols:
        conn.close()
        return {"error": "no assets found", "points": []}

    # Step 2: fetch prices for BTC + symbols over the tight window only
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp::date >= %s
          AND timestamp::date <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (["BTC"] + symbols, dt_from, dt_to_s))
    rows = cur.fetchall()
    conn.close()

    # Build price maps
    prices = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in prices: prices[sym] = {}
        prices[sym][str(row["date"])] = float(row["price_usd"])

    if "BTC" not in prices or len(prices["BTC"]) < 2:
        return {"error": "insufficient BTC data", "points": []}

    btc_vals   = [prices["BTC"][d] for d in sorted(prices["BTC"])]
    btc_return = (btc_vals[-1] / btc_vals[0] - 1) * 100
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


def handle_alt_altseason(params):
    """% of top-N alts (ex-BTC) outperforming BTC over a rolling window."""
    date_from = params.get("from",   ["2024-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["90"])[0])
    topn      = min(int(params.get("topn", ["50"])[0]), 250)

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get top-N alts by recent mcap
    cur.execute("""
        SELECT p.symbol
        FROM price_daily p
        JOIN (
            SELECT coingecko_id, market_cap_usd
            FROM marketcap_daily
            WHERE timestamp::date = (
                SELECT MAX(timestamp::date) FROM marketcap_daily WHERE timestamp <= NOW()
            ) AND market_cap_usd > 0
        ) m ON p.coingecko_id = m.coingecko_id
        WHERE p.symbol NOT IN ('BTC','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')
        GROUP BY p.symbol, m.market_cap_usd
        ORDER BY m.market_cap_usd DESC
        LIMIT %s
    """, (topn,))
    symbols = [r['symbol'] for r in cur.fetchall()]

    if not symbols:
        conn.close()
        return {"dates": [], "values": [], "btc_dominance": [], "reason": "no_symbols"}

    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp::date >= %s AND timestamp::date <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (['BTC'] + symbols, dt_from_ext, date_to))
    rows = cur.fetchall()

    # BTC mcap dominance
    cur.execute("""
        SELECT b.timestamp::date as date,
               b.market_cap_usd as btc_mcap,
               t.total_mcap
        FROM (
            SELECT timestamp::date as date, market_cap_usd
            FROM marketcap_daily
            WHERE coingecko_id IN (SELECT coingecko_id FROM price_daily WHERE symbol='BTC' LIMIT 1)
              AND timestamp >= %s AND timestamp <= %s AND market_cap_usd > 0
        ) b
        JOIN (
            SELECT timestamp::date as date, SUM(market_cap_usd) as total_mcap
            FROM marketcap_daily WHERE timestamp >= %s AND timestamp <= %s AND market_cap_usd > 0
            GROUP BY timestamp::date
        ) t ON b.date = t.date
        ORDER BY b.date
    """, (date_from, date_to, date_from, date_to))
    dom_rows = cur.fetchall()
    conn.close()

    dom_map = {}
    for row in dom_rows:
        t = float(row['total_mcap']) if row['total_mcap'] else 0
        b = float(row['btc_mcap'])   if row['btc_mcap']   else 0
        if t > 0:
            dom_map[str(row['date'])] = round(b / t * 100, 4)

    prices = {}
    for row in rows:
        sym = row['symbol']
        if sym not in prices: prices[sym] = {}
        prices[sym][str(row['date'])] = float(row['price_usd'])

    if 'BTC' not in prices:
        return {"dates": [], "values": [], "btc_dominance": [],
                "reason": f"no_btc_data sym_count={len(symbols)} rows={len(rows)} range={dt_from_ext}..{date_to}"}

    all_dates = sorted(set(d for s in prices.values() for d in s))
    result_dates, result_pcts = [], []

    for d in all_dates:
        if d < date_from:
            continue
        btc_sorted = sorted(prices['BTC'].keys())
        # find index of d in btc history
        btc_dates = sorted(prices['BTC'].keys())
        if d not in prices['BTC']:
            continue
        di = btc_dates.index(d)
        if di < window:
            continue
        start_d = btc_dates[di - window]
        btc_start = prices['BTC'].get(start_d)
        btc_end   = prices['BTC'].get(d)
        if not btc_start or not btc_end or btc_start == 0:
            continue
        btc_ret = btc_end / btc_start - 1

        count_above = 0
        count_total = 0
        for sym in symbols:
            if sym not in prices or d not in prices[sym]:
                continue
            sym_dates = sorted(prices[sym].keys())
            si = sym_dates.index(d) if d in sym_dates else -1
            if si < window:
                continue
            s_start_d = sym_dates[si - window]
            s_start = prices[sym].get(s_start_d)
            s_end   = prices[sym].get(d)
            if not s_start or not s_end or s_start == 0:
                continue
            sym_ret = s_end / s_start - 1
            count_total += 1
            if sym_ret > btc_ret:
                count_above += 1

        if count_total > 0:
            result_dates.append(d)
            result_pcts.append(round(count_above / count_total * 100, 2))

    btc_dom_vals = [dom_map.get(d) for d in result_dates]
    return {"dates": result_dates, "values": result_pcts, "btc_dominance": btc_dom_vals}


def handle_alt_beta(params):
    """OLS beta and alpha of each alt's daily returns vs BTC."""
    date_to = params.get("to",     ["2099-01-01"])[0]
    window  = int(params.get("window", ["60"])[0])
    topn    = min(int(params.get("topn",   ["50"])[0]), 250)

    if date_to == "2099-01-01":
        date_to = datetime.now().strftime("%Y-%m-%d")
    try:
        dt_from = (datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        dt_from = "2024-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT p.symbol, ar.sector
        FROM price_daily p
        LEFT JOIN asset_registry ar ON p.coingecko_id = ar.coingecko_id
        JOIN (
            SELECT coingecko_id, market_cap_usd
            FROM marketcap_daily
            WHERE timestamp::date = (
                SELECT MAX(timestamp::date) FROM marketcap_daily WHERE timestamp <= NOW()
            ) AND market_cap_usd > 0
        ) m ON p.coingecko_id = m.coingecko_id
        WHERE p.symbol NOT IN ('BTC','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')
        GROUP BY p.symbol, ar.sector, m.market_cap_usd
        ORDER BY m.market_cap_usd DESC
        LIMIT %s
    """, (topn,))
    sym_sector = {r['symbol']: r['sector'] for r in cur.fetchall()}
    symbols = list(sym_sector.keys())

    if not symbols:
        conn.close()
        return {"points": []}

    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp::date >= %s AND timestamp::date <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (['BTC'] + symbols, dt_from, date_to))
    rows = cur.fetchall()
    conn.close()

    prices = {}
    for row in rows:
        sym = row['symbol']
        if sym not in prices: prices[sym] = {}
        prices[sym][str(row['date'])] = float(row['price_usd'])

    if 'BTC' not in prices or len(prices['BTC']) < window:
        return {"points": []}

    btc_dates = sorted(prices['BTC'].keys())[-window - 1:]
    btc_rets  = [
        math.log(prices['BTC'][btc_dates[i]] / prices['BTC'][btc_dates[i - 1]])
        for i in range(1, len(btc_dates))
        if prices['BTC'].get(btc_dates[i]) and prices['BTC'].get(btc_dates[i - 1])
    ]
    if len(btc_rets) < 10:
        return {"points": []}

    btc_mean = sum(btc_rets) / len(btc_rets)
    btc_var  = sum((r - btc_mean) ** 2 for r in btc_rets) / len(btc_rets)

    points = []
    for sym in symbols:
        if sym not in prices: continue
        sym_dates_sorted = sorted(prices[sym].keys())
        # align to btc dates
        common = [d for d in btc_dates[1:] if d in prices[sym]]
        if len(common) < 10: continue
        sym_rets_map = {}
        sym_sorted = sorted(prices[sym].keys())
        for i in range(1, len(sym_sorted)):
            d = sym_sorted[i]
            if prices[sym].get(sym_sorted[i - 1]) and prices[sym].get(d):
                sym_rets_map[d] = math.log(prices[sym][d] / prices[sym][sym_sorted[i - 1]])

        paired_btc = []
        paired_sym = []
        for d in common:
            b = None
            bi = btc_dates.index(d) if d in btc_dates else -1
            if bi > 0:
                b = math.log(prices['BTC'][btc_dates[bi]] / prices['BTC'][btc_dates[bi - 1]]) if prices['BTC'].get(btc_dates[bi - 1]) else None
            s = sym_rets_map.get(d)
            if b is not None and s is not None:
                paired_btc.append(b)
                paired_sym.append(s)

        if len(paired_btc) < 10: continue
        bm = sum(paired_btc) / len(paired_btc)
        sm = sum(paired_sym) / len(paired_sym)
        cov = sum((paired_btc[i] - bm) * (paired_sym[i] - sm) for i in range(len(paired_btc))) / len(paired_btc)
        bvar = sum((r - bm) ** 2 for r in paired_btc) / len(paired_btc)
        if bvar == 0: continue
        beta  = cov / bvar
        alpha = (sm - beta * bm) * 365 * 100  # annualized

        sector = sym_sector.get(sym, '')
        color  = SECTOR_COLORS.get(sector, '#888888')
        points.append({
            "symbol": sym,
            "beta":   round(beta, 4),
            "alpha":  round(alpha, 4),
            "mcap":   0,
            "color_sector": color,
        })

    return {"points": points}


def handle_alt_heatmap(params):
    """Pairwise rolling 30d Pearson correlation between selected alts — last value."""
    symbols   = [s.strip().upper() for s in params.get("symbols", [""])[0].split(",") if s.strip()]
    date_from = params.get("from",   ["2024-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    if len(symbols) < 2:
        return {"error": "need >= 2 symbols"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    prices = {}
    for row in rows:
        sym = row['symbol']
        if sym not in prices: prices[sym] = {}
        prices[sym][str(row['date'])] = float(row['price_usd'])

    present = [s for s in symbols if s in prices and len(prices[s]) >= window]
    if len(present) < 2:
        return {"error": "insufficient data"}

    all_dates = sorted(set(d for s in present for d in prices[s]))
    n = len(present)
    matrix = [[None] * n for _ in range(n)]

    for i in range(n):
        matrix[i][i] = 1.0
        for j in range(i + 1, n):
            a, b = present[i], present[j]
            xa = [prices[a].get(d) for d in all_dates]
            xb = [prices[b].get(d) for d in all_dates]
            corr = rolling_corr(xa, xb, window)
            last = next((v for v in reversed(corr) if v is not None), None)
            matrix[i][j] = last
            matrix[j][i] = last

    return {"symbols": present, "matrix": matrix}


def handle_alt_ath_drawdown(params):
    """Current drawdown from all-time high for each asset, sorted worst-first."""
    symbols = [s.strip().upper() for s in params.get("symbols", [""])[0].split(",") if s.strip()]
    topn    = min(int(params.get("topn", ["50"])[0]), 250)

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if not symbols:
        # use top-N by mcap
        cur.execute("""
            SELECT p.symbol
            FROM price_daily p
            JOIN (
                SELECT coingecko_id, market_cap_usd
                FROM marketcap_daily
                WHERE timestamp::date = (
                    SELECT MAX(timestamp::date) FROM marketcap_daily WHERE timestamp <= NOW()
                ) AND market_cap_usd > 0
            ) m ON p.coingecko_id = m.coingecko_id
            WHERE p.symbol NOT IN ('BTC','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')
            GROUP BY p.symbol, m.market_cap_usd
            ORDER BY m.market_cap_usd DESC
            LIMIT %s
        """, (topn,))
        symbols = [r['symbol'] for r in cur.fetchall()]

    if not symbols:
        conn.close()
        return {"points": []}

    cur.execute("""
        SELECT symbol,
               MAX(price_usd) as ath_price,
               (array_agg(price_usd ORDER BY timestamp DESC))[1] as current_price
        FROM price_daily
        WHERE symbol = ANY(%s) AND price_usd > 0
        GROUP BY symbol
    """, (symbols,))
    rows = cur.fetchall()
    conn.close()

    points = []
    for row in rows:
        ath     = float(row['ath_price'])
        current = float(row['current_price'])
        if ath > 0:
            dd = round((current / ath - 1) * 100, 2)
            points.append({
                "symbol":        row['symbol'],
                "drawdown_pct":  dd,
                "current_price": current,
                "ath_price":     ath,
            })

    points.sort(key=lambda p: p['drawdown_pct'])
    return {"points": points}


def handle_alt_funding_heatmap(params):
    """Daily avg funding rate per asset — heatmap matrix."""
    symbols   = [s.strip().upper() for s in params.get("symbols", [""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    if not symbols:
        return {"symbols": [], "dates": [], "matrix": {}}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT symbol, timestamp::date as date, AVG(funding_rate) as avg_rate
        FROM funding_8h
        WHERE symbol = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
        GROUP BY symbol, timestamp::date
        ORDER BY symbol, timestamp::date
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    data = {}
    for row in rows:
        sym = row['symbol']
        if sym not in data: data[sym] = {}
        data[sym][str(row['date'])] = float(row['avg_rate']) if row['avg_rate'] is not None else None

    present = [s for s in symbols if s in data]
    if not present:
        return {"symbols": [], "dates": [], "matrix": {}}

    all_dates = sorted(set(d for s in present for d in data[s]))
    matrix = {}
    for sym in present:
        matrix[sym] = [data[sym].get(d) for d in all_dates]

    return {"symbols": present, "dates": all_dates, "matrix": matrix}


def handle_alt_drawdown_ts(params):
    """
    Max drawdown over time for top-N altcoins.
    Returns {dates: [...], series: {SYM: {dates: [...], drawdowns: [...]}, ...}}
    Each value in drawdowns is the % drawdown from the running ATH at that date.
    """
    topn      = min(int(params.get("topn", ["20"])[0]), 100)
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT p.symbol, p.coingecko_id
        FROM price_daily p
        JOIN (
            SELECT coingecko_id, market_cap_usd
            FROM marketcap_daily
            WHERE timestamp::date = (
                SELECT MAX(timestamp::date) FROM marketcap_daily WHERE timestamp <= NOW()
            ) AND market_cap_usd > 0
        ) m ON p.coingecko_id = m.coingecko_id
        WHERE p.symbol NOT IN ('BTC','USDT','USDC','DAI','BUSD','TUSD','USDP','FDUSD','PYUSD')
        GROUP BY p.symbol, p.coingecko_id, m.market_cap_usd
        ORDER BY m.market_cap_usd DESC
        LIMIT %s
    """, (topn,))
    symbols = [r['symbol'] for r in cur.fetchall()]

    if not symbols:
        conn.close()
        return {"dates": [], "series": {}}

    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    prices = {}
    for row in rows:
        sym = row['symbol']
        if sym not in prices:
            prices[sym] = {'dates': [], 'values': []}
        prices[sym]['dates'].append(str(row['date']))
        prices[sym]['values'].append(float(row['price_usd']))

    series = {}
    all_dates = set()
    for sym in symbols:
        if sym not in prices or len(prices[sym]['values']) < 2:
            continue
        dates = prices[sym]['dates']
        vals  = prices[sym]['values']
        ath = vals[0]
        drawdowns = []
        for v in vals:
            if v > ath:
                ath = v
            dd = round((v / ath - 1) * 100, 2) if ath > 0 else 0
            drawdowns.append(dd)
        series[sym] = {'dates': dates, 'drawdowns': drawdowns}
        all_dates.update(dates)

    return {"dates": sorted(all_dates), "series": series}


def handle_alt_rebase(params):
    """Rebased performance of selected tokens to 100."""
    symbols_raw = params.get("symbols", [""])[0]
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    date_from = params.get("from", ["2024-04-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    if not symbols: return {"error": "no symbols"}

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s) AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    result = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in result: result[sym] = {"dates": [], "prices": []}
        result[sym]["dates"].append(str(r["date"]))
        result[sym]["prices"].append(float(r["price_usd"]))

    # Rebase each to 100
    for sym in result:
        prices = result[sym]["prices"]
        first = next((p for p in prices if p > 0), None)
        if first:
            result[sym]["rebased"] = [round(p / first * 100, 4) for p in prices]
        else:
            result[sym]["rebased"] = prices
        del result[sym]["prices"]

    return {"assets": result}


def handle_alt_zscore_momentum(params):
    """Z-scored momentum: 14d return / 14d vol, z-scored over 90d rolling window."""
    symbols_raw = params.get("symbols", [""])[0]
    symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    date_from = params.get("from", ["2024-04-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    if not symbols: return {"error": "no symbols"}

    import math
    from datetime import timedelta

    try:
        ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=120)).strftime("%Y-%m-%d")
    except:
        ext = "2023-01-01"

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s) AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (symbols, ext, date_to))
    rows = cur.fetchall()
    conn.close()

    # Build per-symbol price arrays
    price_maps = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in price_maps: price_maps[sym] = {}
        price_maps[sym][str(r["date"])] = float(r["price_usd"])

    RET_WINDOW = 14
    VOL_WINDOW = 14
    Z_WINDOW = 90

    result = {}
    for sym, pmap in price_maps.items():
        sorted_dates = sorted(pmap.keys())
        prices = [pmap[d] for d in sorted_dates]

        # Daily log returns
        log_rets = [None]
        for i in range(1, len(prices)):
            if prices[i] > 0 and prices[i-1] > 0:
                log_rets.append(math.log(prices[i] / prices[i-1]))
            else:
                log_rets.append(None)

        # 14d return and 14d vol
        momentum = []
        for i in range(len(prices)):
            if i < RET_WINDOW:
                momentum.append(None)
                continue
            # 14d return
            if prices[i] > 0 and prices[i - RET_WINDOW] > 0:
                ret14 = math.log(prices[i] / prices[i - RET_WINDOW])
            else:
                momentum.append(None)
                continue

            # 14d vol
            rets = [r for r in log_rets[i - VOL_WINDOW + 1:i + 1] if r is not None]
            if len(rets) < VOL_WINDOW // 2:
                momentum.append(None)
                continue
            mean_r = sum(rets) / len(rets)
            vol = math.sqrt(sum((r - mean_r)**2 for r in rets) / len(rets))

            if vol > 0:
                momentum.append(ret14 / vol)
            else:
                momentum.append(None)

        # Z-score over 90d rolling
        z_dates = []
        z_vals = []
        for i in range(Z_WINDOW, len(momentum)):
            if momentum[i] is None: continue
            d = sorted_dates[i]
            if d < date_from: continue

            window = [m for m in momentum[i - Z_WINDOW + 1:i + 1] if m is not None]
            if len(window) < Z_WINDOW // 2: continue

            mean_m = sum(window) / len(window)
            std_m = math.sqrt(sum((m - mean_m)**2 for m in window) / len(window))

            if std_m > 0:
                z = (momentum[i] - mean_m) / std_m
            else:
                z = 0

            z_dates.append(d)
            z_vals.append(round(z, 4))

        if z_dates:
            result[sym] = {
                "dates": z_dates,
                "zscore": z_vals,
                "current": z_vals[-1],
            }

    return {"assets": result}
