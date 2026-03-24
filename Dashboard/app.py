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

    if tab == "majors":
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
    cast = ts_cast(granularity)
    cur.execute(f"""
        SELECT p.coingecko_id, {cast} as ts, p.price_usd, m.market_cap_usd
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
            elif p.startswith("/static/"):       self.send_file(BASE_DIR/p[8:])
            else:
                self.send_response(404); self.end_headers()
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] running on port {PORT}")
    server.serve_forever()
