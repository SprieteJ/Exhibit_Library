"""
api/sector.py — sector price, correlation, momentum, bubble, mcap
"""
import math
from datetime import datetime, timedelta
from collections import defaultdict
from api.shared import (get_conn, SECTORS, SECTOR_COLORS,
                     rebase_series, rolling_corr, fetch_sector_index,
                     price_table, ts_cast)
import psycopg2.extras


def handle_sectors():
    return [{"name": n, "count": len(ids), "color": SECTOR_COLORS.get(n, "#888888")}
            for n, ids in SECTORS.items()]


def handle_sector_price(params, weighting='equal'):
    sectors     = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from", ["2024-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    granularity = params.get("granularity", ["daily"])[0]
    align       = params.get("align", ["own"])[0]
    if not sectors: return {"error": "no sectors"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, weighting, granularity)
        if dates:
            result[sector] = {
                "dates":   dates,
                "rebased": [index[d] for d in dates],
                "count":   len(SECTORS[sector]),
                "color":   SECTOR_COLORS.get(sector),
            }

    if align == "common" and len(result) > 1:
        common_start = max(v["dates"][0] for v in result.values())
        for k in result:
            i = next((i for i,d in enumerate(result[k]["dates"]) if d >= common_start), 0)
            result[k]["dates"]   = result[k]["dates"][i:]
            result[k]["rebased"] = rebase_series(result[k]["rebased"][i:])

    conn.close()
    return result


def handle_intra_corr(params):
    sectors     = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from", ["2024-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    window      = int(params.get("window", ["30"])[0])
    granularity = params.get("granularity", ["daily"])[0]
    if len(sectors) < 2: return {"error": "need >= 2 sectors"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    sector_series = {}
    all_dates_set = None

    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, 'equal', granularity)
        if dates:
            sector_series[sector] = index
            s = set(dates)
            all_dates_set = s if all_dates_set is None else all_dates_set & s
    conn.close()

    if not all_dates_set or len(sector_series) < 2:
        return {"error": "insufficient data"}

    common_dates = sorted(all_dates_set)
    result = {}
    sec_list = list(sector_series.keys())

    for i in range(len(sec_list)):
        for j in range(i+1, len(sec_list)):
            a, b = sec_list[i], sec_list[j]
            xa   = [sector_series[a].get(d) for d in common_dates]
            xb   = [sector_series[b].get(d) for d in common_dates]
            corr = rolling_corr(xa, xb, window)
            result[f"{a} / {b}"] = {
                "dates":   common_dates,
                "rebased": corr,
                "count":   0,
                "color":   SECTOR_COLORS.get(a),
            }
    return result


def handle_btc_corr(params):
    sectors     = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from", ["2024-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    window      = int(params.get("window", ["30"])[0])
    versus      = params.get("versus", ["BTC"])[0].upper()
    granularity = params.get("granularity", ["daily"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cast = ts_cast(granularity)
    tbl  = price_table(granularity)

    cur.execute(f"""
        SELECT {cast} as ts, price_usd FROM {tbl}
        WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (versus, date_from, date_to))
    ref_rows = {str(r["ts"]): float(r["price_usd"]) for r in cur.fetchall()}

    result = {}
    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, 'equal', granularity)
        if not dates: continue
        common = sorted(set(dates) & set(ref_rows.keys()))
        if len(common) < window: continue
        xs   = [index.get(d) for d in common]
        ys   = [ref_rows.get(d) for d in common]
        corr = rolling_corr(xs, ys, window)
        result[f"{sector} vs {versus}"] = {
            "dates":   common,
            "rebased": corr,
            "count":   0,
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_momentum(params):
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window+10)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], dt_from_ext, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 1: continue

        idx_vals = [index[d] for d in dates]
        mom_dates, mom_values = [], []

        for i in range(window, len(dates)):
            prev = idx_vals[i - window]
            curr = idx_vals[i]
            if prev and prev > 0:
                mom_dates.append(dates[i])
                mom_values.append(round((curr / prev - 1) * 100, 4))

        trimmed = [(d, v) for d, v in zip(mom_dates, mom_values) if d >= date_from]
        if not trimmed: continue

        td, tv = zip(*trimmed)
        result[sector] = {
            "dates":   list(td),
            "rebased": list(tv),
            "count":   len(SECTORS[sector]),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_zscore(params):
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window*2)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], dt_from_ext, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 2: continue

        idx_vals = [index[d] for d in dates]
        returns  = [None] + [
            (idx_vals[i] / idx_vals[i-1] - 1) * 100
            if idx_vals[i-1] and idx_vals[i-1] > 0 else None
            for i in range(1, len(idx_vals))
        ]

        z_dates, z_values = [], []
        for i in range(window, len(dates)):
            wr       = [r for r in returns[i-window:i] if r is not None]
            curr_ret = returns[i]
            if len(wr) < window // 2 or curr_ret is None: continue
            mean = sum(wr) / len(wr)
            std  = math.sqrt(sum((r-mean)**2 for r in wr) / len(wr))
            if std > 0:
                z_dates.append(dates[i])
                z_values.append(round((curr_ret - mean) / std, 4))

        trimmed = [(d, v) for d, v in zip(z_dates, z_values) if d >= date_from]
        if not trimmed: continue

        td, tv = zip(*trimmed)
        result[sector] = {
            "dates":   list(td),
            "rebased": list(tv),
            "count":   len(SECTORS[sector]),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_bubble(params):
    date_to = params.get("to",  ["2099-01-01"])[0]
    window  = int(params.get("window", ["30"])[0])

    try:
        dt_from = (datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=window*3)).strftime("%Y-%m-%d")
    except:
        dt_from = "2024-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector, cg_ids in SECTORS.items():
        if not cg_ids: continue
        index, dates = fetch_sector_index(cur, cg_ids, dt_from, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 2: continue

        idx_vals = [index[d] for d in dates]
        returns  = [
            (idx_vals[i] / idx_vals[i-1] - 1) * 100
            if idx_vals[i-1] and idx_vals[i-1] > 0 else None
            for i in range(1, len(idx_vals))
        ]

        # Momentum: N-day return
        momentum = None
        if len(idx_vals) >= window + 1:
            pv = idx_vals[-(window+1)]; cv = idx_vals[-1]
            if pv and pv > 0:
                momentum = round((cv / pv - 1) * 100, 4)

        # Autocorrelation lag-1 over window
        rets = [r for r in returns[-window:] if r is not None]
        autocorr = None
        if len(rets) >= window // 2:
            pairs = [(rets[i], rets[i+1]) for i in range(len(rets)-1)]
            if pairs:
                xa = sum(p[0] for p in pairs) / len(pairs)
                ya = sum(p[1] for p in pairs) / len(pairs)
                num = sum((p[0]-xa)*(p[1]-ya) for p in pairs)
                dx  = math.sqrt(sum((p[0]-xa)**2 for p in pairs))
                dy  = math.sqrt(sum((p[1]-ya)**2 for p in pairs))
                if dx > 0 and dy > 0:
                    autocorr = round(num / (dx * dy), 4)

        # Total market cap
        cur.execute("""
            SELECT SUM(latest_mcap) as total_mcap FROM (
                SELECT DISTINCT ON (coingecko_id) market_cap_usd as latest_mcap
                FROM marketcap_daily
                WHERE coingecko_id = ANY(%s) AND market_cap_usd > 0
                ORDER BY coingecko_id, timestamp DESC
            ) sub
        """, (cg_ids,))
        mcap_row   = cur.fetchone()
        total_mcap = float(mcap_row["total_mcap"]) if mcap_row and mcap_row["total_mcap"] else 0

        result[sector] = {
            "x":     autocorr,
            "y":     momentum,
            "mcap":  total_mcap,
            "color": SECTOR_COLORS.get(sector, "#888888"),
            "count": len(cg_ids),
        }

    conn.close()
    return result


def handle_sector_dominance(params):
    """100% stacked area: each sector's mcap as % of total per day."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Collect all sector mcaps per date
    sector_daily = {}  # sector -> {date: mcap}
    for sector, cg_ids in SECTORS.items():
        if not cg_ids:
            continue
        cur.execute("""
            SELECT timestamp::date as date, SUM(market_cap_usd) as mcap
            FROM marketcap_daily
            WHERE coingecko_id = ANY(%s)
              AND timestamp >= %s AND timestamp <= %s
              AND market_cap_usd > 0
            GROUP BY timestamp::date
            ORDER BY timestamp::date
        """, (cg_ids, date_from, date_to))
        rows = cur.fetchall()
        sector_daily[sector] = {str(r["date"]): float(r["mcap"]) for r in rows}

    conn.close()
    if not sector_daily:
        return {}

    all_dates = sorted(set(d for s in sector_daily.values() for d in s))
    if not all_dates:
        return {}

    result = {}
    for sector in sector_daily:
        pcts = []
        for d in all_dates:
            total = sum(sector_daily[s].get(d, 0) for s in sector_daily)
            mine  = sector_daily[sector].get(d, 0)
            pcts.append(round(mine / total * 100, 4) if total > 0 else 0)
        result[sector] = {
            "dates":  all_dates,
            "values": pcts,
            "color":  SECTOR_COLORS.get(sector, "#888888"),
        }
    return result


def handle_sector_xheatmap(params):
    """NxN cross-sector rolling correlation heatmap (last value of rolling window)."""
    date_from = params.get("from",   ["2024-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sector_series = {}
    all_dates_set = None
    for sector, cg_ids in SECTORS.items():
        if not cg_ids:
            continue
        index, dates = fetch_sector_index(cur, cg_ids, date_from, date_to, 'equal', 'daily')
        if dates:
            sector_series[sector] = index
            s = set(dates)
            all_dates_set = s if all_dates_set is None else all_dates_set & s

    conn.close()
    if not all_dates_set or len(sector_series) < 2:
        return {"error": "insufficient data"}

    common_dates = sorted(all_dates_set)
    sectors = sorted(sector_series.keys())
    n = len(sectors)
    matrix = [[None] * n for _ in range(n)]

    for i in range(n):
        matrix[i][i] = 1.0
        for j in range(i + 1, n):
            a, b = sectors[i], sectors[j]
            xa = [sector_series[a].get(d) for d in common_dates]
            xb = [sector_series[b].get(d) for d in common_dates]
            corr = rolling_corr(xa, xb, window)
            last = next((v for v in reversed(corr) if v is not None), None)
            matrix[i][j] = last
            matrix[j][i] = last

    return {"sectors": sectors, "matrix": matrix}


def handle_sector_cumulative(params):
    """Cumulative return per sector over the chosen period — sorted bar chart."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector, cg_ids in SECTORS.items():
        if not cg_ids:
            continue
        index, dates = fetch_sector_index(cur, cg_ids, date_from, date_to, 'equal', 'daily')
        if not dates or len(dates) < 2:
            continue
        first = index[dates[0]]
        last  = index[dates[-1]]
        if first and first > 0:
            result[sector] = {
                "value": round((last / first - 1) * 100, 4),
                "color": SECTOR_COLORS.get(sector, "#888888"),
            }

    conn.close()
    return result


def handle_sector_vol(params):
    """30d rolling annualized realized vol of EW daily log returns."""
    sectors   = [s.strip() for s in params.get("sectors", [""])[0].split(",") if s.strip()]
    date_from = params.get("from",   ["2024-01-01"])[0]
    date_to   = params.get("to",     ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS:
            continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], dt_from_ext, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 2:
            continue

        idx_vals = [index[d] for d in dates]
        log_rets = [None] + [
            math.log(idx_vals[i] / idx_vals[i - 1])
            if idx_vals[i] and idx_vals[i - 1] and idx_vals[i - 1] > 0 else None
            for i in range(1, len(idx_vals))
        ]

        vol_dates, vol_values = [], []
        for i in range(window, len(dates)):
            wr = [r for r in log_rets[i - window:i] if r is not None]
            if len(wr) < window // 2:
                continue
            mean = sum(wr) / len(wr)
            std  = math.sqrt(sum((r - mean) ** 2 for r in wr) / len(wr))
            ann  = std * math.sqrt(365) * 100
            vol_dates.append(dates[i])
            vol_values.append(round(ann, 4))

        trimmed = [(d, v) for d, v in zip(vol_dates, vol_values) if d >= date_from]
        if not trimmed:
            continue
        td, tv = zip(*trimmed)
        result[sector] = {
            "dates":   list(td),
            "rebased": list(tv),
            "count":   len(SECTORS[sector]),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_drawdown(params):
    """Rolling drawdown from peak of EW sector index."""
    sectors   = [s.strip() for s in params.get("sectors", [""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS:
            continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, 'equal', 'daily')
        if not dates:
            continue

        idx_vals = [index[d] for d in dates]
        running_max = idx_vals[0]
        dd_values = []
        for v in idx_vals:
            if v > running_max:
                running_max = v
            dd = (v / running_max - 1) * 100 if running_max > 0 else 0
            dd_values.append(round(dd, 4))

        result[sector] = {
            "dates":   dates,
            "rebased": dd_values,
            "count":   len(SECTORS[sector]),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_breadth(params):
    """% of constituents above their 50d SMA per sector per day."""
    sectors   = [s.strip() for s in params.get("sectors", [""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    sma_win   = 50

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=sma_win + 10)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS:
            continue
        cg_ids = SECTORS[sector]
        if not cg_ids:
            continue

        cur.execute("""
            SELECT coingecko_id, timestamp::date as date, price_usd
            FROM price_daily
            WHERE coingecko_id = ANY(%s)
              AND timestamp >= %s AND timestamp <= %s
              AND price_usd > 0
            ORDER BY coingecko_id, timestamp::date
        """, (cg_ids, dt_from_ext, date_to))
        rows = cur.fetchall()
        if not rows:
            continue

        asset_prices = defaultdict(dict)
        for row in rows:
            asset_prices[row['coingecko_id']][str(row['date'])] = float(row['price_usd'])

        all_dates = sorted(set(d for ap in asset_prices.values() for d in ap))
        breadth_dates, breadth_values = [], []

        for d in all_dates:
            if d < date_from:
                continue
            above = 0
            total = 0
            for cid, prices in asset_prices.items():
                if d not in prices:
                    continue
                sorted_d = sorted(prices.keys())
                idx = sorted_d.index(d) if d in sorted_d else -1
                if idx < sma_win:
                    continue
                window_prices = [prices[sorted_d[k]] for k in range(idx - sma_win, idx)]
                sma = sum(window_prices) / len(window_prices)
                total += 1
                if prices[d] > sma:
                    above += 1
            if total > 0:
                breadth_dates.append(d)
                breadth_values.append(round(above / total * 100, 4))

        if not breadth_dates:
            continue
        result[sector] = {
            "dates":   breadth_dates,
            "rebased": breadth_values,
            "count":   len(cg_ids),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_funding(params):
    """Average 8h funding rate by sector per day."""
    sectors   = [s.strip() for s in params.get("sectors", [""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS:
            continue
        cg_ids = SECTORS[sector]
        if not cg_ids:
            continue

        # Get symbols for these coingecko_ids from asset_registry or funding_8h
        cur.execute("""
            SELECT DISTINCT symbol FROM funding_8h
            WHERE coingecko_id = ANY(%s)
        """, (cg_ids,))
        syms = [r['symbol'] for r in cur.fetchall()]
        if not syms:
            continue

        cur.execute("""
            SELECT timestamp::date as date, AVG(funding_rate) as avg_rate
            FROM funding_8h
            WHERE coingecko_id = ANY(%s)
              AND timestamp >= %s AND timestamp <= %s
            GROUP BY timestamp::date
            ORDER BY timestamp::date
        """, (cg_ids, date_from, date_to))
        rows = cur.fetchall()
        if not rows:
            continue

        result[sector] = {
            "dates":   [str(r['date']) for r in rows],
            "rebased": [float(r['avg_rate']) if r['avg_rate'] is not None else None for r in rows],
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_oi(params):
    """Sum of open interest (USD) across sector constituents per day."""
    sectors   = [s.strip() for s in params.get("sectors", [""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS:
            continue
        cg_ids = SECTORS[sector]
        if not cg_ids:
            continue

        cur.execute("""
            SELECT timestamp::date as date, SUM(oi_usd) as total_oi
            FROM open_interest_daily
            WHERE coingecko_id = ANY(%s)
              AND timestamp >= %s AND timestamp <= %s
            GROUP BY timestamp::date
            ORDER BY timestamp::date
        """, (cg_ids, date_from, date_to))
        rows = cur.fetchall()
        if not rows:
            continue

        result[sector] = {
            "dates":   [str(r['date']) for r in rows],
            "rebased": [float(r['total_oi']) if r['total_oi'] is not None else None for r in rows],
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


def handle_sector_sharpe(params):
    """Scatter: x=30d vol, y=30d return for each sector at 'to' date."""
    date_to = params.get("to",     ["2099-01-01"])[0]
    window  = int(params.get("window", ["30"])[0])

    if date_to == "2099-01-01":
        date_to = datetime.now().strftime("%Y-%m-%d")

    try:
        dt_from = (datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        dt_from = "2024-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector, cg_ids in SECTORS.items():
        if not cg_ids:
            continue
        index, dates = fetch_sector_index(cur, cg_ids, dt_from, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 2:
            continue

        idx_vals = [index[d] for d in dates]
        log_rets = [
            math.log(idx_vals[i] / idx_vals[i - 1])
            if idx_vals[i] and idx_vals[i - 1] and idx_vals[i - 1] > 0 else None
            for i in range(1, len(idx_vals))
        ]
        recent = [r for r in log_rets[-window:] if r is not None]
        if len(recent) < window // 2:
            continue

        mean = sum(recent) / len(recent)
        std  = math.sqrt(sum((r - mean) ** 2 for r in recent) / len(recent))
        ann_vol    = std * math.sqrt(365) * 100
        # Return = cumulative over window
        cum_ret = (idx_vals[-1] / idx_vals[-(window + 1)] - 1) * 100 if idx_vals[-(window + 1)] > 0 else None
        if cum_ret is None:
            continue

        result[sector] = {
            "x":     round(ann_vol, 4),
            "y":     round(cum_ret, 4),
            "color": SECTOR_COLORS.get(sector, "#888888"),
        }

    conn.close()
    return result


def handle_sector_rrg(params):
    """Relative Rotation Graph — RS-Ratio (x) vs RS-Momentum (y) per sector."""
    rs_window   = int(params.get("window",    ["10"])[0])
    mom_window  = int(params.get("momentum",  ["6"])[0])
    granularity = params.get("granularity", ["daily"])[0]
    benchmark   = params.get("benchmark",  ["market"])[0].lower()
    tail_len    = int(params.get("tail",      ["0"])[0])
    date_to     = params.get("to", [datetime.now().strftime("%Y-%m-%d")])[0]
    if date_to == "2099-01-01":
        date_to = datetime.now().strftime("%Y-%m-%d")

    needed = rs_window + mom_window + tail_len + 15
    if granularity == "weekly":
        needed *= 7
    needed += 30

    try:
        dt_from = (datetime.strptime(date_to, "%Y-%m-%d") - timedelta(days=needed)).strftime("%Y-%m-%d")
    except Exception:
        dt_from = "2022-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch all sector indices (daily)
    sector_data  = {}
    all_dates_set = None
    for sector, cg_ids in SECTORS.items():
        if not cg_ids: continue
        index, dates = fetch_sector_index(cur, cg_ids, dt_from, date_to, 'equal', 'daily')
        if not dates: continue
        sector_data[sector] = index
        s = set(dates)
        all_dates_set = s if all_dates_set is None else all_dates_set & s

    if not all_dates_set:
        conn.close()
        return {"error": "no sector data"}

    common_dates = sorted(all_dates_set)

    # Build benchmark series
    if benchmark in ('btc', 'eth'):
        sym = 'BTC' if benchmark == 'btc' else 'ETH'
        cur.execute("""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
            ORDER BY timestamp
        """, (sym, dt_from, date_to))
        bm_map = {str(r["date"]): float(r["price_usd"]) for r in cur.fetchall()}
        common_dates = [d for d in common_dates if d in bm_map]
        bm_series    = {d: bm_map[d] for d in common_dates}
    else:
        bm_series = {}
        for d in common_dates:
            vals = [sector_data[s][d] for s in sector_data if sector_data[s].get(d)]
            if vals:
                bm_series[d] = sum(vals) / len(vals)
        common_dates = [d for d in common_dates if bm_series.get(d)]

    conn.close()

    if len(common_dates) < rs_window + mom_window + 2:
        return {"error": "insufficient data for these parameters"}

    if granularity == "weekly":
        common_dates = common_dates[::7]

    if len(common_dates) < rs_window + mom_window + 2:
        return {"error": "insufficient data after downsampling"}

    result = {}
    for sector, index in sector_data.items():
        rs = []
        for d in common_dates:
            sv = index.get(d); bv = bm_series.get(d)
            rs.append(sv / bv if sv and bv and bv > 0 else None)

        rs_ratio = [None] * len(rs)
        for i in range(rs_window, len(rs)):
            if rs[i] and rs[i - rs_window] and rs[i - rs_window] > 0:
                rs_ratio[i] = 100.0 + (rs[i] / rs[i - rs_window] - 1.0) * 100.0

        rs_mom = [None] * len(rs_ratio)
        for i in range(mom_window, len(rs_ratio)):
            if rs_ratio[i] and rs_ratio[i - mom_window] and rs_ratio[i - mom_window] > 0:
                rs_mom[i] = 100.0 + (rs_ratio[i] / rs_ratio[i - mom_window] - 1.0) * 100.0

        valid = [i for i in range(len(common_dates)) if rs_ratio[i] and rs_mom[i]]
        if not valid: continue

        ci = valid[-1]
        x, y = round(rs_ratio[ci], 4), round(rs_mom[ci], 4)
        quadrant = ("Leading"   if x >= 100 and y >= 100 else
                    "Improving" if x <  100 and y >= 100 else
                    "Lagging"   if x <  100 and y <  100 else
                    "Weakening")
        tail = [{"x": round(rs_ratio[ti], 4), "y": round(rs_mom[ti], 4)}
                for ti in valid[-(tail_len + 1):-1]] if tail_len > 0 else []

        result[sector] = {
            "x": x, "y": y,
            "color":    SECTOR_COLORS.get(sector, "#888888"),
            "quadrant": quadrant,
            "tail":     tail,
        }

    return result


def handle_sector_mcap_view(params):
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    mcap_type = params.get("type", ["total"])[0]
    if not sectors: return {"error": "no sectors"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        cg_ids = SECTORS[sector]
        if not cg_ids: continue

        cur.execute("""
            SELECT timestamp::date as date,
                   SUM(market_cap_usd) as total_mcap,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY market_cap_usd) as median_mcap
            FROM marketcap_daily
            WHERE coingecko_id = ANY(%s)
              AND timestamp >= %s AND timestamp <= %s
              AND market_cap_usd > 0
            GROUP BY timestamp::date
            ORDER BY timestamp::date
        """, (cg_ids, date_from, date_to))
        rows = cur.fetchall()
        if not rows: continue

        dates  = [str(r["date"]) for r in rows]
        values = [float(r["total_mcap"] if mcap_type == "total" else r["median_mcap"]) for r in rows]

        result[sector] = {
            "dates":   dates,
            "rebased": values,
            "color":   SECTOR_COLORS.get(sector),
            "count":   len(cg_ids),
        }

    conn.close()
    return result
