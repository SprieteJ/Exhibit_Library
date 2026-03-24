#!/usr/bin/env python3
"""
app.py — Wintermute Dashboard API
All endpoints support ?granularity=daily|hourly
"""

import os, json, urllib.parse, math
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from collections import defaultdict
import psycopg2, psycopg2.extras

PORT         = int(os.environ.get("PORT", 8080))
DB_URL       = os.environ.get("DATABASE_URL", "")
BASE_DIR     = Path(__file__).parent
SECTORS_FILE = BASE_DIR / "sectors.json"

def load_sectors():
    if SECTORS_FILE.exists():
        with open(SECTORS_FILE) as f: return json.load(f)
    return {}

SECTORS = load_sectors()

# Fixed sector colours
SECTOR_COLORS = {
    "Memecoins":   "#F7931A",
    "Layer 1":     "#2471CC",
    "Layer 2":     "#746BE6",
    "DeFi":        "#00D64A",
    "DePIN":       "#DB33CB",
    "Gaming":      "#EC5B5B",
    "AI":          "#26A17B",
    "Stablecoins": "#888B88",
}

# Tab asset definitions
MAJORS    = ["BTC", "ETH"]
MACRO_TICKERS = ["SPY","QQQ","IWM","DIA","TLT","IEF","SHY","GLD","SLV","BNO","USO","DX-Y.NYB","EURUSD=X","JPYUSD=X","^VIX","^TNX","^IRX","^TYX"]

def get_conn(): return psycopg2.connect(DB_URL)

def rebase_series(prices):
    first = next((p for p in prices if p is not None and not math.isnan(p)), None)
    if not first or first == 0: return prices
    return [round(p / first * 100, 4) if p is not None else None for p in prices]

def rolling_corr(x, y, window):
    n = len(x)
    result = [None] * n
    for i in range(window - 1, n):
        xs = x[i-window+1:i+1]; ys = y[i-window+1:i+1]
        pairs = [(a,b) for a,b in zip(xs,ys) if a is not None and b is not None]
        if len(pairs) < window // 2: continue
        xa = sum(p[0] for p in pairs)/len(pairs); ya = sum(p[1] for p in pairs)/len(pairs)
        num = sum((p[0]-xa)*(p[1]-ya) for p in pairs)
        dx = math.sqrt(sum((p[0]-xa)**2 for p in pairs))
        dy = math.sqrt(sum((p[1]-ya)**2 for p in pairs))
        if dx > 0 and dy > 0: result[i] = round(num/(dx*dy), 4)
    return result

def price_table(granularity):
    return "price_hourly" if granularity == "hourly" else "price_daily"

def mcap_table(granularity):
    return "marketcap_daily"  # no hourly mcap

def macro_table(granularity):
    return "macro_hourly" if granularity == "hourly" else "macro_daily"

def ts_cast(granularity):
    return "timestamp" if granularity == "hourly" else "timestamp::date"

# ── Individual price ──────────────────────────────────────────────────────────
def handle_price(params):
    symbols     = [s.strip().upper() for s in params.get("symbols",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from",["2024-01-01"])[0]
    date_to     = params.get("to",  ["2099-01-01"])[0]
    granularity = params.get("granularity",["daily"])[0]
    align       = params.get("align",["own"])[0]  # own | common
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
            idx = next((i for i,d in enumerate(data[sym]["dates"]) if d >= common_start), 0)
            data[sym]["dates"]  = data[sym]["dates"][idx:]
            data[sym]["prices"] = data[sym]["prices"][idx:]

    for sym in data:
        data[sym]["rebased"] = rebase_series(data[sym]["prices"])
    return data

# ── Assets list ───────────────────────────────────────────────────────────────
def handle_assets(params):
    tab = params.get("tab",["individual"])[0]
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if tab in ("majors", "bitcoin"):
        cur.execute("""
            SELECT DISTINCT p.symbol, r.coingecko_name as name
            FROM price_daily p LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
            WHERE p.symbol = ANY(%s) ORDER BY p.symbol
        """, (MAJORS,))
    elif tab == "altcoins":
        cur.execute("""
            SELECT DISTINCT p.symbol, r.coingecko_name as name
            FROM price_daily p LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
            WHERE p.symbol != ALL(%s) ORDER BY p.symbol
        """, (MAJORS,))
    elif tab == "macro":
        cur.execute("""
            SELECT DISTINCT ticker as symbol, name FROM macro_daily
            WHERE ticker = ANY(%s) ORDER BY ticker
        """, (MACRO_TICKERS,))
    else:
        cur.execute("""
            SELECT DISTINCT p.symbol, r.coingecko_name as name
            FROM price_daily p LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
            ORDER BY p.symbol
        """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── Macro price ───────────────────────────────────────────────────────────────
def handle_macro_price(params):
    symbols     = [s.strip() for s in params.get("symbols",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from",["2024-01-01"])[0]
    date_to     = params.get("to",  ["2099-01-01"])[0]
    granularity = params.get("granularity",["daily"])[0]
    align       = params.get("align",["own"])[0]
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

    data = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in data: data[sym] = {"dates": [], "prices": []}
        data[sym]["dates"].append(str(row["ts"]))
        data[sym]["prices"].append(float(row["price_usd"]) if row["price_usd"] else None)

    if align == "common" and len(data) > 1:
        common_start = max(s["dates"][0] for s in data.values() if s["dates"])
        for sym in data:
            idx = next((i for i,d in enumerate(data[sym]["dates"]) if d >= common_start), 0)
            data[sym]["dates"]  = data[sym]["dates"][idx:]
            data[sym]["prices"] = data[sym]["prices"][idx:]

    for sym in data:
        data[sym]["rebased"] = rebase_series(data[sym]["prices"])
    return data

# ── Sectors ───────────────────────────────────────────────────────────────────
def handle_sectors():
    return [{"name": n, "count": len(ids), "color": SECTOR_COLORS.get(n,"#888888")} for n,ids in SECTORS.items()]

def fetch_sector_index(cur, cg_ids, date_from, date_to, weighting='equal', granularity='daily'):
    tbl  = price_table(granularity)
    if granularity == "hourly":
        # Hourly: no mcap join (mcap is daily only), use p.timestamp directly
        cur.execute(f"""
            SELECT p.coingecko_id, p.timestamp as ts, p.price_usd, NULL::double precision as market_cap_usd
            FROM {tbl} p
            WHERE p.coingecko_id = ANY(%s) AND p.timestamp >= %s AND p.timestamp <= %s AND p.price_usd > 0
            ORDER BY p.coingecko_id, p.timestamp
        """, (cg_ids, date_from, date_to))
    else:
        cur.execute(f"""
            SELECT p.coingecko_id, p.timestamp::date as ts, p.price_usd, m.market_cap_usd
            FROM {tbl} p
            LEFT JOIN marketcap_daily m ON p.coingecko_id = m.coingecko_id AND p.timestamp::date = m.timestamp::date
            WHERE p.coingecko_id = ANY(%s) AND p.timestamp >= %s AND p.timestamp <= %s AND p.price_usd > 0
            ORDER BY p.coingecko_id, p.timestamp
        """, (cg_ids, date_from, date_to))
    rows = cur.fetchall()
    if not rows: return {}, []

    asset_prices = defaultdict(dict)
    asset_mcaps  = defaultdict(dict)
    for row in rows:
        cid = row['coingecko_id']; d = str(row['ts'])
        asset_prices[cid][d] = float(row['price_usd'])
        if row['market_cap_usd']: asset_mcaps[cid][d] = float(row['market_cap_usd'])

    all_dates = sorted(set(d for s in asset_prices.values() for d in s))
    if not all_dates: return {}, []

    rebased = {}
    for cid, prices in asset_prices.items():
        sorted_d = sorted(prices); first = prices[sorted_d[0]]
        if first > 0: rebased[cid] = {d: prices[d]/first*100 for d in sorted_d}

    if not rebased: return {}, []
    min_assets = max(1, len(rebased) * 0.5)
    index = {}

    for date in all_dates:
        if weighting == 'mcap':
            vals_w = [(series[date], asset_mcaps.get(cid,{}).get(date,0))
                      for cid, series in rebased.items() if date in series and asset_mcaps.get(cid,{}).get(date,0) > 0]
            if len(vals_w) >= min_assets:
                tw = sum(w for _,w in vals_w)
                index[date] = round(sum(v*w/tw for v,w in vals_w), 4)
        else:
            vals = [s[date] for s in rebased.values() if date in s]
            if len(vals) >= min_assets: index[date] = round(sum(vals)/len(vals), 4)

    if index:
        sd = sorted(index); first = index[sd[0]]
        if first > 0: index = {d: round(v/first*100,4) for d,v in index.items()}

    return index, sorted(index.keys())

def handle_sector_price(params, weighting='equal'):
    sectors     = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from",["2024-01-01"])[0]
    date_to     = params.get("to",  ["2099-01-01"])[0]
    granularity = params.get("granularity",["daily"])[0]
    align       = params.get("align",["own"])[0]
    if not sectors: return {"error": "no sectors"}

    conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, weighting, granularity)
        if dates:
            result[sector] = {"dates": dates, "rebased": [index[d] for d in dates],
                              "count": len(SECTORS[sector]), "color": SECTOR_COLORS.get(sector)}

    if align == "common" and len(result) > 1:
        common_start = max(v["dates"][0] for v in result.values())
        for k in result:
            idx = next((i for i,d in enumerate(result[k]["dates"]) if d >= common_start), 0)
            result[k]["dates"]   = result[k]["dates"][idx:]
            result[k]["rebased"] = result[k]["rebased"][idx:]
            result[k]["rebased"] = rebase_series(result[k]["rebased"])

    conn.close()
    return result

def handle_intra_corr(params):
    sectors     = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from",["2024-01-01"])[0]
    date_to     = params.get("to",  ["2099-01-01"])[0]
    window      = int(params.get("window",["30"])[0])
    granularity = params.get("granularity",["daily"])[0]
    if len(sectors) < 2: return {"error": "need >= 2 sectors"}

    conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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

    if not all_dates_set or len(sector_series) < 2: return {"error": "insufficient data"}
    common_dates = sorted(all_dates_set)
    result = {}
    sec_list = list(sector_series.keys())
    for i in range(len(sec_list)):
        for j in range(i+1, len(sec_list)):
            a, b = sec_list[i], sec_list[j]
            xa = [sector_series[a].get(d) for d in common_dates]
            xb = [sector_series[b].get(d) for d in common_dates]
            corr = rolling_corr(xa, xb, window)
            result[f"{a} / {b}"] = {"dates": common_dates, "rebased": corr, "count": 0,
                                     "color": SECTOR_COLORS.get(a)}
    return result

def handle_btc_corr(params):
    sectors     = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from   = params.get("from",["2024-01-01"])[0]
    date_to     = params.get("to",  ["2099-01-01"])[0]
    window      = int(params.get("window",["30"])[0])
    versus      = params.get("versus",["BTC"])[0].upper()
    granularity = params.get("granularity",["daily"])[0]

    conn = get_conn(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cast = ts_cast(granularity); tbl = price_table(granularity)
    cur.execute(f"""
        SELECT {cast} as ts, price_usd FROM {tbl}
        WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s ORDER BY timestamp
    """, (versus, date_from, date_to))
    ref_rows = {str(r["ts"]): float(r["price_usd"]) for r in cur.fetchall()}

    result = {}
    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, 'equal', granularity)
        if not dates: continue
        common = sorted(set(dates) & set(ref_rows.keys()))
        if len(common) < window: continue
        xs = [index.get(d) for d in common]; ys = [ref_rows.get(d) for d in common]
        corr = rolling_corr(xs, ys, window)
        result[f"{sector} vs {versus}"] = {"dates": common, "rebased": corr, "count": 0,
                                            "color": SECTOR_COLORS.get(sector)}
    conn.close()
    return result

# ── Sector momentum ──────────────────────────────────────────────────────────
def handle_sector_momentum(params):
    """
    Rolling N-day return of the equal-weighted sector index.
    momentum[i] = (index[i] / index[i-window]) - 1, expressed as %
    """
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from",["2024-01-01"])[0]
    date_to   = params.get("to",  ["2099-01-01"])[0]
    window    = int(params.get("window",["30"])[0])

    # Fetch extra history before date_from so rolling window is populated at start
    from datetime import datetime, timedelta
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_from_ext = (dt_from - timedelta(days=window + 10)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], dt_from_ext, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 1: continue

        # Compute rolling N-day return
        mom_dates  = []
        mom_values = []
        idx_vals   = [index[d] for d in dates]

        for i in range(window, len(dates)):
            prev = idx_vals[i - window]
            curr = idx_vals[i]
            if prev and prev > 0:
                ret = round((curr / prev - 1) * 100, 4)
                mom_dates.append(dates[i])
                mom_values.append(ret)

        # Trim to requested date_from
        trimmed_dates  = []
        trimmed_values = []
        for d, v in zip(mom_dates, mom_values):
            if d >= date_from:
                trimmed_dates.append(d)
                trimmed_values.append(v)

        if not trimmed_dates: continue

        result[sector] = {
            "dates":   trimmed_dates,
            "rebased": trimmed_values,  # reusing rebased field for chart compat
            "count":   len(SECTORS[sector]),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result


# ── Sector z-score momentum ──────────────────────────────────────────────────
def handle_sector_zscore(params):
    """
    Z-score of daily returns vs rolling window.
    z[i] = (r[i] - mean(r[i-w:i])) / std(r[i-w:i])
    Tells you how unusual today's return is vs recent history.
    """
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from",["2024-01-01"])[0]
    date_to   = params.get("to",  ["2099-01-01"])[0]
    window    = int(params.get("window",["30"])[0])

    from datetime import datetime, timedelta
    try:
        dt_from     = datetime.strptime(date_from, "%Y-%m-%d")
        dt_from_ext = (dt_from - timedelta(days=window * 2)).strftime("%Y-%m-%d")
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

        # Daily returns
        returns = [None]
        for i in range(1, len(idx_vals)):
            prev = idx_vals[i-1]
            curr = idx_vals[i]
            if prev and prev > 0:
                returns.append((curr / prev - 1) * 100)
            else:
                returns.append(None)

        # Rolling z-score of returns
        z_dates  = []
        z_values = []

        for i in range(window, len(dates)):
            window_rets = [r for r in returns[i-window:i] if r is not None]
            if len(window_rets) < window // 2: continue
            curr_ret = returns[i]
            if curr_ret is None: continue

            mean = sum(window_rets) / len(window_rets)
            var  = sum((r - mean)**2 for r in window_rets) / len(window_rets)
            std  = math.sqrt(var) if var > 0 else None

            if std and std > 0:
                z_dates.append(dates[i])
                z_values.append(round((curr_ret - mean) / std, 4))

        # Trim to requested date_from
        trimmed_dates  = [d for d in z_dates  if d >= date_from]
        trimmed_values = [v for d, v in zip(z_dates, z_values) if d >= date_from]

        if not trimmed_dates: continue

        result[sector] = {
            "dates":   trimmed_dates,
            "rebased": trimmed_values,
            "count":   len(SECTORS[sector]),
            "color":   SECTOR_COLORS.get(sector),
        }

    conn.close()
    return result



# ── DB status ─────────────────────────────────────────────────────────────────
def handle_db_status():
    """
    Returns metadata for each table: row count, min/max timestamp, last ingested_at.
    Used by the Data tab to show freshness and coverage.
    """
    from datetime import datetime, timezone, timedelta

    TABLES = [
        {"key": "price_daily",          "label": "Price",             "granularity": "Daily",    "source": "CoinGecko",      "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "price_hourly",          "label": "Price",             "granularity": "Hourly",   "source": "CoinGecko",      "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "marketcap_daily",       "label": "Market cap",        "granularity": "Daily",    "source": "CoinGecko",      "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "volume_daily",          "label": "Volume",            "granularity": "Daily",    "source": "CoinGecko",      "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "funding_8h",            "label": "Funding rate",      "granularity": "8h",       "source": "Binance/Bybit",  "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "open_interest_daily",   "label": "Open interest",     "granularity": "Daily",    "source": "Binance/Bybit",  "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "open_interest_hourly",  "label": "Open interest",     "granularity": "Hourly",   "source": "Binance/Bybit",  "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "long_short_ratio",      "label": "Long/short ratio",  "granularity": "Daily/1h", "source": "Binance/Bybit",  "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "macro_daily",           "label": "Macro assets",      "granularity": "Daily",    "source": "yfinance",       "asset_col": "ticker",       "ts_col": "timestamp"},
        {"key": "macro_hourly",          "label": "Macro assets",      "granularity": "Hourly",   "source": "yfinance",       "asset_col": "ticker",       "ts_col": "timestamp"},
        {"key": "asset_registry",        "label": "GMCI asset classification", "granularity": "Static", "source": "Internal", "asset_col": "symbol",      "ts_col": None},
    ]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    now  = datetime.now(timezone.utc)
    result = []

    for t in TABLES:
        try:
            if t["ts_col"]:
                cur.execute(f"""
                    SELECT
                        COUNT(*) as rows,
                        COUNT(DISTINCT {t["asset_col"]}) as assets,
                        MIN({t["ts_col"]})::date as date_from,
                        MAX({t["ts_col"]})::date as date_to,
                        MAX(ingested_at) as last_updated
                    FROM {t["key"]}
                """)
            else:
                cur.execute(f"""
                    SELECT
                        COUNT(*) as rows,
                        COUNT(DISTINCT {t["asset_col"]}) as assets,
                        NULL as date_from,
                        NULL as date_to,
                        NULL as last_updated
                    FROM {t["key"]}
                """)
            row = cur.fetchone()

            # Determine freshness
            last_updated = row["last_updated"]
            if last_updated is None:
                status = "manual"
            else:
                if last_updated.tzinfo is None:
                    last_updated = last_updated.replace(tzinfo=timezone.utc)
                age_hours = (now - last_updated).total_seconds() / 3600
                status = "live" if age_hours <= 48 else "stale"

            result.append({
                "label":       t["label"],
                "granularity": t["granularity"],
                "source":      t["source"],
                "rows":        int(row["rows"]),
                "assets":      int(row["assets"]),
                "date_from":   str(row["date_from"]) if row["date_from"] else "—",
                "date_to":     str(row["date_to"])   if row["date_to"]   else "—",
                "last_updated": last_updated.strftime("%Y-%m-%d %H:%M") if last_updated else "—",
                "status":      status,
            })
        except Exception as e:
            result.append({
                "label": t["label"], "granularity": t["granularity"],
                "source": t["source"], "rows": 0, "assets": 0,
                "date_from": "—", "date_to": "—", "last_updated": "—",
                "status": "error", "error": str(e),
            })

    conn.close()
    return result


# ── Sector bubble (momentum + autocorr + mcap) ────────────────────────────────
def handle_sector_bubble(params):
    """
    Returns per-sector snapshot for bubble chart:
      x = autocorrelation (lag-1 of daily returns over window)
      y = rolling N-day momentum (%)
      r = total market cap of sector (sum of constituent mcaps)
    """
    date_to   = params.get("to",  ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    from datetime import datetime, timedelta
    try:
        dt_to     = datetime.strptime(date_to, "%Y-%m-%d")
        dt_from   = (dt_to - timedelta(days=window * 3)).strftime("%Y-%m-%d")
    except:
        dt_from   = "2024-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector, cg_ids in SECTORS.items():
        if not cg_ids: continue

        # Get sector index
        index, dates = fetch_sector_index(cur, cg_ids, dt_from, date_to, 'equal', 'daily')
        if not dates or len(dates) < window + 2: continue

        idx_vals = [index[d] for d in dates]

        # Daily returns
        returns = []
        for i in range(1, len(idx_vals)):
            prev = idx_vals[i-1]
            if prev and prev > 0:
                returns.append((idx_vals[i] / prev - 1) * 100)
            else:
                returns.append(None)

        # Momentum: return over last N days
        if len(idx_vals) >= window + 1:
            prev_val = idx_vals[-(window+1)]
            curr_val = idx_vals[-1]
            momentum = round((curr_val / prev_val - 1) * 100, 4) if prev_val and prev_val > 0 else None
        else:
            momentum = None

        # Autocorrelation (lag-1) over window
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

        # Total market cap (sum of latest mcap per asset)
        cur.execute("""
            SELECT SUM(latest_mcap) as total_mcap FROM (
                SELECT DISTINCT ON (coingecko_id)
                    market_cap_usd as latest_mcap
                FROM marketcap_daily
                WHERE coingecko_id = ANY(%s)
                  AND market_cap_usd > 0
                ORDER BY coingecko_id, timestamp DESC
            ) sub
        """, (cg_ids,))
        mcap_row = cur.fetchone()
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


# ── Sector market cap ─────────────────────────────────────────────────────────
def handle_sector_mcap_view(params):
    """
    Returns time series of total and median market cap per sector.
    type = total | median
    """
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    mcap_type = params.get("type", ["total"])[0]  # total | median

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
            "rebased": values,   # raw USD values, not rebased
            "color":   SECTOR_COLORS.get(sector),
            "count":   len(cg_ids),
        }

    conn.close()
    return result


# ── BTC halving epochs ────────────────────────────────────────────────────────
def handle_btc_epochs(params):
    """
    BTC price as x-fold from halving price, indexed to 1.0 at halving date.
    Epoch 3 (2016-07-09), Epoch 4 (2020-05-11), Epoch 5 (2024-04-20)
    """
    days_to_show = int(params.get("days", ["1400"])[0])

    HALVINGS = {
        "Epoch 3 (2016)": "2016-07-09",
        "Epoch 4 (2020)": "2020-05-11",
        "Epoch 5 (2024)": "2024-04-20",
    }

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, halving_date in HALVINGS.items():
        cur.execute("""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE symbol = 'BTC'
              AND timestamp::date >= %s::date
              AND timestamp::date <= (%s::date + INTERVAL '%s days')
              AND price_usd > 0
            ORDER BY timestamp
        """, (halving_date, halving_date, days_to_show))
        rows = cur.fetchall()
        if not rows: continue

        # Find halving price (first row on or after halving date)
        halving_price = float(rows[0]["price_usd"])
        if halving_price == 0: continue

        days_list  = []
        xfold_list = []
        for row in rows:
            from datetime import datetime
            d    = row["date"]
            days = (d - datetime.strptime(halving_date, "%Y-%m-%d").date()).days
            if 0 <= days <= days_to_show:
                days_list.append(days)
                xfold_list.append(round(float(row["price_usd"]) / halving_price, 6))

        if days_list:
            result[label] = {
                "days":    days_list,
                "values":  xfold_list,
                "halving_price": halving_price,
            }

    conn.close()
    return result


# ── BTC bear market cycles ────────────────────────────────────────────────────
def handle_btc_cycles(params):
    """
    BTC price indexed to 100 at cycle peak, days since peak.
    2017/18 peak: 2017-12-17, 2021/22 peak: 2021-11-10, 2025 peak: user-adjustable
    """
    days_to_show = int(params.get("days", ["1000"])[0])
    peak_2025    = params.get("peak2025", ["2025-01-20"])[0]

    PEAKS = {
        "2017/18 Bear": "2017-12-17",
        "2021/22 Bear": "2021-11-10",
        "2025 Bear (ongoing)": peak_2025,
    }

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, peak_date in PEAKS.items():
        cur.execute("""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE symbol = 'BTC'
              AND timestamp::date >= %s::date
              AND timestamp::date <= (%s::date + INTERVAL '%s days')
              AND price_usd > 0
            ORDER BY timestamp
        """, (peak_date, peak_date, days_to_show))
        rows = cur.fetchall()
        if not rows: continue

        peak_price = float(rows[0]["price_usd"])
        if peak_price == 0: continue

        days_list    = []
        indexed_list = []
        for row in rows:
            from datetime import datetime
            d    = row["date"]
            days = (d - datetime.strptime(peak_date, "%Y-%m-%d").date()).days
            if 0 <= days <= days_to_show:
                days_list.append(days)
                indexed_list.append(round(float(row["price_usd"]) / peak_price * 100, 4))

        if days_list:
            result[label] = {
                "days":       days_list,
                "values":     indexed_list,
                "peak_price": peak_price,
                "peak_date":  peak_date,
            }

    conn.close()
    return result


# ── BTC Halving Epochs ────────────────────────────────────────────────────────
def handle_btc_epochs(params):
    """
    Returns x-fold performance from halving date for Epochs 3, 4, 5.
    x = days since halving, y = price / halving_price (x-fold)
    Uses price_daily table.
    """
    from datetime import datetime, timedelta

    HALVINGS = {
        "Epoch 3 (2016)": datetime(2016, 7, 9),
        "Epoch 4 (2020)": datetime(2020, 5, 11),
        "Epoch 5 (2024)": datetime(2024, 4, 20),
    }
    DAYS = 1400

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, halving_date in HALVINGS.items():
        date_from = (halving_date - timedelta(days=2)).strftime("%Y-%m-%d")
        date_to   = min(halving_date + timedelta(days=DAYS+2), datetime.today()).strftime("%Y-%m-%d")

        cur.execute("""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE coingecko_id = 'bitcoin'
              AND timestamp >= %s AND timestamp <= %s
              AND price_usd > 0
            ORDER BY timestamp
        """, (date_from, date_to))
        rows = cur.fetchall()
        if not rows: continue

        dates  = [r["date"] for r in rows]
        prices = [float(r["price_usd"]) for r in rows]

        # Find halving price (closest date)
        import bisect
        idx = bisect.bisect_left(dates, halving_date.date())
        idx = min(idx, len(prices) - 1)
        halving_price = prices[idx]
        if not halving_price: continue

        # Days since halving + x-fold
        x_vals, y_vals = [], []
        for d, p in zip(dates, prices):
            day = (d - halving_date.date()).days
            if 0 <= day <= DAYS:
                x_vals.append(day)
                y_vals.append(round(p / halving_price, 4))

        result[label] = {"x": x_vals, "y": y_vals}

    conn.close()
    return result


# ── BTC Bear Market Cycles ────────────────────────────────────────────────────
def handle_btc_cycles(params):
    """
    Returns price indexed to peak (peak=100) for each bear cycle.
    x = days since peak, y = indexed price
    """
    from datetime import datetime, timedelta

    PEAKS = {
        "2017/18 Bear": datetime(2017, 12, 17),
        "2021/22 Bear": datetime(2021, 11, 10),
        "2025 Bear (ongoing)": datetime(2025, 10, 6),
    }
    DAYS = 1000

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, peak_date in PEAKS.items():
        date_from = (peak_date - timedelta(days=2)).strftime("%Y-%m-%d")
        date_to   = min(peak_date + timedelta(days=DAYS+2), datetime.today()).strftime("%Y-%m-%d")

        cur.execute("""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE coingecko_id = 'bitcoin'
              AND timestamp >= %s AND timestamp <= %s
              AND price_usd > 0
            ORDER BY timestamp
        """, (date_from, date_to))
        rows = cur.fetchall()
        if not rows: continue

        dates  = [r["date"] for r in rows]
        prices = [float(r["price_usd"]) for r in rows]

        import bisect
        idx = bisect.bisect_left(dates, peak_date.date())
        idx = min(idx, len(prices) - 1)
        peak_price = prices[idx]
        if not peak_price: continue

        x_vals, y_vals = [], []
        for d, p in zip(dates, prices):
            day = (d - peak_date.date()).days
            if 0 <= day <= DAYS:
                x_vals.append(day)
                y_vals.append(round(p / peak_price * 100, 4))

        result[label] = {"x": x_vals, "y": y_vals}

    conn.close()
    return result


# ── BTC vs TradFi ─────────────────────────────────────────────────────────────
def handle_btc_tradfi(params):
    """
    Two-panel data for BTC vs a TradFi asset:
      - Rolling N-day return for both
      - Rolling M-day correlation of daily returns
    Returns from price_daily (BTC) and macro_daily (TradFi assets).
    """
    asset       = params.get("asset",   ["QQQ"])[0]
    perf_window = int(params.get("perf",  ["14"])[0])
    corr_window = int(params.get("corr",  ["14"])[0])
    smooth_win  = int(params.get("smooth",["60"])[0])
    date_from   = params.get("from", ["2023-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]

    # Extend date_from back by max window to populate rolling calcs
    from datetime import datetime, timedelta
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_ext  = (dt_from - timedelta(days=max(perf_window, corr_window, smooth_win) + 10)).strftime("%Y-%m-%d")
    except:
        dt_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch BTC daily prices
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (dt_ext, date_to))
    btc_rows = {str(r["date"]): float(r["price_usd"]) for r in cur.fetchall()}

    # Fetch TradFi asset from macro_daily
    cur.execute("""
        SELECT timestamp::date as date, close
        FROM macro_daily
        WHERE ticker = %s AND timestamp >= %s AND timestamp <= %s AND close IS NOT NULL
        ORDER BY timestamp
    """, (asset, dt_ext, date_to))
    asset_rows = {str(r["date"]): float(r["close"]) for r in cur.fetchall()}

    conn.close()

    if not btc_rows or not asset_rows:
        return {"error": f"no data for BTC or {asset}"}

    # Align on common dates
    common_dates = sorted(set(btc_rows.keys()) & set(asset_rows.keys()))
    if len(common_dates) < corr_window + 2:
        return {"error": "insufficient overlapping data"}

    btc_prices   = [btc_rows[d]   for d in common_dates]
    asset_prices = [asset_rows[d] for d in common_dates]

    # Daily returns
    def daily_returns(prices):
        rets = [None]
        for i in range(1, len(prices)):
            if prices[i-1] and prices[i-1] > 0:
                rets.append((prices[i] / prices[i-1] - 1) * 100)
            else:
                rets.append(None)
        return rets

    btc_rets   = daily_returns(btc_prices)
    asset_rets = daily_returns(asset_prices)

    # Rolling N-day performance (cumulative return over window)
    def rolling_perf(prices, window):
        result = [None] * len(prices)
        for i in range(window, len(prices)):
            prev = prices[i - window]
            curr = prices[i]
            if prev and prev > 0:
                result[i] = round((curr / prev - 1) * 100, 4)
        return result

    btc_perf   = rolling_perf(btc_prices, perf_window)
    asset_perf = rolling_perf(asset_prices, perf_window)

    # Rolling correlation of daily returns
    def rolling_corr_series(r1, r2, window):
        result = [None] * len(r1)
        for i in range(window, len(r1)):
            x = r1[i-window:i]; y = r2[i-window:i]
            pairs = [(a,b) for a,b in zip(x,y) if a is not None and b is not None]
            if len(pairs) < window // 2: continue
            xa = sum(p[0] for p in pairs)/len(pairs)
            ya = sum(p[1] for p in pairs)/len(pairs)
            num = sum((p[0]-xa)*(p[1]-ya) for p in pairs)
            dx  = math.sqrt(sum((p[0]-xa)**2 for p in pairs))
            dy  = math.sqrt(sum((p[1]-ya)**2 for p in pairs))
            if dx > 0 and dy > 0:
                result[i] = round(num/(dx*dy), 4)
        return result

    corr_series   = rolling_corr_series(btc_rets, asset_rets, corr_window)

    # Smooth correlation
    def rolling_mean(series, window):
        result = [None] * len(series)
        for i in range(window, len(series)):
            vals = [v for v in series[i-window:i] if v is not None]
            if vals: result[i] = round(sum(vals)/len(vals), 4)
        return result

    corr_smooth = rolling_mean(corr_series, smooth_win)

    # Trim to requested date_from
    trim_idx = next((i for i,d in enumerate(common_dates) if d >= date_from), 0)
    dates    = common_dates[trim_idx:]

    return {
        "dates":       dates,
        "btc_perf":    btc_perf[trim_idx:],
        "asset_perf":  asset_perf[trim_idx:],
        "corr":        corr_series[trim_idx:],
        "corr_smooth": corr_smooth[trim_idx:],
        "asset":       asset,
        "perf_window": perf_window,
        "corr_window": corr_window,
        "smooth_win":  smooth_win,
    }


# ── BTC vs TradFi ─────────────────────────────────────────────────────────────
def handle_btc_tradfi(params):
    """
    Dual panel: rolling return + rolling correlation between BTC and a macro asset.
    Returns both series aligned on common dates.
    """
    asset      = params.get("asset",   ["QQQ"])[0].upper()
    perf_win   = int(params.get("perf_win",  ["14"])[0])
    corr_win   = int(params.get("corr_win",  ["14"])[0])
    smooth_win = int(params.get("smooth_win",["60"])[0])
    date_from  = params.get("from", ["2023-01-01"])[0]
    date_to    = params.get("to",   ["2099-01-01"])[0]

    from datetime import datetime, timedelta
    # Fetch extra history for rolling windows
    try:
        dt_from = datetime.strptime(date_from, "%Y-%m-%d")
        dt_ext  = (dt_from - timedelta(days=max(perf_win, corr_win, smooth_win) + 10)).strftime("%Y-%m-%d")
    except:
        dt_ext  = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # BTC daily prices
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (dt_ext, date_to))
    btc_rows = {str(r["date"]): float(r["price_usd"]) for r in cur.fetchall()}

    # Macro asset prices
    cur.execute("""
        SELECT timestamp::date as date, close as price
        FROM macro_daily
        WHERE ticker = %s AND timestamp >= %s AND timestamp <= %s AND close IS NOT NULL
        ORDER BY timestamp
    """, (asset, dt_ext, date_to))
    mac_rows = {str(r["date"]): float(r["price"]) for r in cur.fetchall()}

    conn.close()

    if not btc_rows or not mac_rows:
        return {"error": f"No data for BTC or {asset}"}

    # Common dates
    common_dates = sorted(set(btc_rows.keys()) & set(mac_rows.keys()))
    if len(common_dates) < max(perf_win, corr_win) + 2:
        return {"error": "Insufficient overlapping data"}

    btc_prices = [btc_rows[d] for d in common_dates]
    mac_prices = [mac_rows[d] for d in common_dates]

    # Daily returns
    def daily_rets(prices):
        rets = [None]
        for i in range(1, len(prices)):
            p, c = prices[i-1], prices[i]
            rets.append((c / p - 1) * 100 if p > 0 else None)
        return rets

    btc_rets = daily_rets(btc_prices)
    mac_rets = daily_rets(mac_prices)

    # Rolling N-day return: (price[i] / price[i-N]) - 1
    def rolling_perf(prices, window):
        result = [None] * len(prices)
        for i in range(window, len(prices)):
            prev = prices[i - window]
            curr = prices[i]
            if prev and prev > 0:
                result[i] = round((curr / prev - 1) * 100, 4)
        return result

    # Rolling correlation
    def rolling_corr_series(rets_a, rets_b, window):
        result = [None] * len(rets_a)
        for i in range(window, len(rets_a)):
            xa = [r for r in rets_a[i-window:i] if r is not None]
            xb = [r for r in rets_b[i-window:i] if r is not None]
            pairs = [(a, b) for a, b in zip(xa, xb)]
            if len(pairs) < window // 2: continue
            ma = sum(p[0] for p in pairs) / len(pairs)
            mb = sum(p[1] for p in pairs) / len(pairs)
            num = sum((p[0]-ma)*(p[1]-mb) for p in pairs)
            da  = math.sqrt(sum((p[0]-ma)**2 for p in pairs))
            db  = math.sqrt(sum((p[1]-mb)**2 for p in pairs))
            if da > 0 and db > 0:
                result[i] = round(num / (da * db), 4)
        return result

    # Rolling average (smooth)
    def rolling_avg(series, window):
        result = [None] * len(series)
        for i in range(window - 1, len(series)):
            vals = [v for v in series[i-window+1:i+1] if v is not None]
            if vals: result[i] = round(sum(vals) / len(vals), 4)
        return result

    btc_perf  = rolling_perf(btc_prices, perf_win)
    mac_perf  = rolling_perf(mac_prices, perf_win)
    corr      = rolling_corr_series(btc_rets, mac_rets, corr_win)
    corr_smooth = rolling_avg(corr, smooth_win)

    # Trim to requested date_from
    trim_dates   = [d for d in common_dates if d >= date_from]
    trim_start   = common_dates.index(trim_dates[0]) if trim_dates else 0

    def trim(lst): return [lst[i] for i in range(trim_start, len(common_dates))]

    return {
        "dates":       trim(common_dates),
        "btc_perf":    trim(btc_perf),
        "asset_perf":  trim(mac_perf),
        "corr":        trim(corr),
        "corr_smooth": trim(corr_smooth),
        "asset":       asset,
        "perf_win":    perf_win,
        "corr_win":    corr_win,
        "smooth_win":  smooth_win,
    }


# ── Altcoin scatter: performance vs BTC + volatility vs BTC ──────────────────
def handle_alt_scatter(params):
    """
    For top N altcoins (by market cap, excluding BTC/ETH):
      y = % return vs BTC over window
      x = volatility of daily returns relative to BTC volatility
    Returns list of {symbol, perf, vol} points.
    """
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    days      = int(params.get("days", ["7"])[0])
    topn      = int(params.get("topn", ["50"])[0])

    from datetime import datetime, timedelta
    try:
        dt_to   = datetime.strptime(date_to, "%Y-%m-%d")
        dt_from = (dt_to - timedelta(days=days + 5)).strftime("%Y-%m-%d")
    except:
        dt_from = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get top N altcoins by market cap (exclude BTC, ETH)
    cur.execute("""
        SELECT DISTINCT ON (p.symbol) p.symbol, p.coingecko_id, m.market_cap_usd
        FROM price_daily p
        JOIN marketcap_daily m ON p.coingecko_id = m.coingecko_id
            AND p.timestamp::date = m.timestamp::date
        WHERE p.symbol NOT IN ('BTC', 'ETH')
          AND m.market_cap_usd > 0
        ORDER BY p.symbol, m.timestamp DESC
    """)
    all_assets = cur.fetchall()
    # Sort by market cap, take top N
    all_assets = sorted(all_assets, key=lambda r: r["market_cap_usd"] or 0, reverse=True)[:topn]
    symbols    = [r["symbol"] for r in all_assets]

    # Get daily prices for BTC + all altcoins over window
    symbols_with_btc = ["BTC"] + symbols
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY symbol, timestamp
    """, (symbols_with_btc, dt_from, date_to))
    rows = cur.fetchall()
    conn.close()

    # Build price series per symbol
    prices = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in prices: prices[sym] = {}
        prices[sym][str(row["date"])] = float(row["price_usd"])

    if "BTC" not in prices or len(prices["BTC"]) < 2:
        return {"error": "insufficient BTC data", "points": []}

    btc_dates = sorted(prices["BTC"].keys())

    # Compute BTC return and vol over window
    btc_vals = [prices["BTC"][d] for d in btc_dates if d in prices["BTC"]]
    btc_return = (btc_vals[-1] / btc_vals[0] - 1) * 100 if len(btc_vals) >= 2 else 0

    btc_rets = [(btc_vals[i] / btc_vals[i-1] - 1) * 100 for i in range(1, len(btc_vals))]
    btc_mean = sum(btc_rets) / len(btc_rets) if btc_rets else 0
    btc_vol  = math.sqrt(sum((r - btc_mean)**2 for r in btc_rets) / len(btc_rets)) if btc_rets else 1

    points = []
    for sym in symbols:
        if sym not in prices or len(prices[sym]) < 2: continue
        sym_vals = [prices[sym][d] for d in sorted(prices[sym].keys())]
        sym_return = (sym_vals[-1] / sym_vals[0] - 1) * 100

        sym_rets = [(sym_vals[i] / sym_vals[i-1] - 1) * 100 for i in range(1, len(sym_vals))]
        sym_mean = sum(sym_rets) / len(sym_rets) if sym_rets else 0
        sym_vol  = math.sqrt(sum((r - sym_mean)**2 for r in sym_rets) / len(sym_rets)) if sym_rets else 0

        perf_vs_btc = sym_return - btc_return
        vol_vs_btc  = sym_vol - btc_vol if btc_vol > 0 else sym_vol

        points.append({
            "symbol": sym,
            "perf":   round(perf_vs_btc, 2),
            "vol":    round(vol_vs_btc, 2),
        })

    return {"points": points, "btc_return": round(btc_return, 2)}

# ── Handler ───────────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path):
        if not path.exists():
            self.send_response(404); self.end_headers(); return
        mime = {".html":"text/html",".css":"text/css",".js":"application/javascript",
                ".json":"application/json",".png":"image/png",".ico":"image/x-icon"
               }.get(path.suffix.lower(), "application/octet-stream")
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        p = parsed.path
        try:
            if p in ("/","/index.html"):         self.send_file(BASE_DIR/"index.html")
            elif p == "/api/assets":             self.send_json(handle_assets(params))
            elif p == "/api/price":              self.send_json(handle_price(params))
            elif p == "/api/macro-price":        self.send_json(handle_macro_price(params))
            elif p == "/api/sectors":            self.send_json(handle_sectors())
            elif p == "/api/sector-price":       self.send_json(handle_sector_price(params,"equal"))
            elif p == "/api/sector-mcap":        self.send_json(handle_sector_price(params,"mcap"))
            elif p == "/api/sector-intra-corr":  self.send_json(handle_intra_corr(params))
            elif p == "/api/sector-btc-corr":    self.send_json(handle_btc_corr(params))
            elif p == "/api/sector-momentum":        self.send_json(handle_sector_momentum(params))
            elif p == "/api/sector-zscore":          self.send_json(handle_sector_zscore(params))
            elif p == "/api/db-status":           self.send_json(handle_db_status())
            elif p == "/api/btc-tradfi":           self.send_json(handle_btc_tradfi(params))
            elif p == "/api/alt-scatter":          self.send_json(handle_alt_scatter(params))
            elif p == "/api/btc-epochs":           self.send_json(handle_btc_epochs(params))
            elif p == "/api/btc-cycles":           self.send_json(handle_btc_cycles(params))
            elif p == "/api/btc-tradfi":           self.send_json(handle_btc_tradfi(params))
            elif p == "/api/alt-scatter":          self.send_json(handle_alt_scatter(params))
            elif p == "/api/btc-epochs":            self.send_json(handle_btc_epochs(params))
            elif p == "/api/btc-cycles":            self.send_json(handle_btc_cycles(params))
            elif p == "/api/sector-bubble":         self.send_json(handle_sector_bubble(params))
            elif p == "/api/sector-mcap-view":      self.send_json(handle_sector_mcap_view(params))
            elif p.startswith("/static/"):       self.send_file(BASE_DIR/p[8:])
            else:
                self.send_response(404); self.end_headers()
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] running on port {PORT}")
    server.serve_forever()
