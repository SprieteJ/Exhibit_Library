"""
api/sector.py — sector price, correlation, momentum, bubble, mcap
"""
import math
from datetime import datetime, timedelta
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
