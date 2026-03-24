#!/usr/bin/env python3
"""
app.py — Wintermute Dashboard API
Endpoints:
  GET /                       → index.html
  GET /api/assets             → asset list
  GET /api/price              → individual rebased price
  GET /api/sectors            → sector list
  GET /api/sector-price       → equal-weighted sector index
  GET /api/sector-mcap        → marketcap-weighted sector index
  GET /api/sector-intra-corr  → rolling pairwise correlation between sectors
  GET /api/sector-btc-corr    → rolling correlation of sectors vs BTC/ETH/Alts
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

def get_conn(): return psycopg2.connect(DB_URL)


# ── Helpers ───────────────────────────────────────────────────────────────────
def rebase(prices):
    first = next((p for p in prices if p is not None and not math.isnan(p)), None)
    if not first: return prices
    return [round(p / first * 100, 4) if p is not None else None for p in prices]


def rolling_corr(x, y, window):
    """Compute rolling Pearson correlation between two equal-length lists."""
    n = len(x)
    result = [None] * n
    for i in range(window - 1, n):
        xs = x[i - window + 1:i + 1]
        ys = y[i - window + 1:i + 1]
        pairs = [(a, b) for a, b in zip(xs, ys) if a is not None and b is not None]
        if len(pairs) < window // 2:
            continue
        xa = sum(p[0] for p in pairs) / len(pairs)
        ya = sum(p[1] for p in pairs) / len(pairs)
        num = sum((p[0] - xa) * (p[1] - ya) for p in pairs)
        dx  = math.sqrt(sum((p[0] - xa) ** 2 for p in pairs))
        dy  = math.sqrt(sum((p[1] - ya) ** 2 for p in pairs))
        if dx > 0 and dy > 0:
            result[i] = round(num / (dx * dy), 4)
    return result


def fetch_sector_index(cur, cg_ids, date_from, date_to, weighting='equal'):
    """
    Returns {date_str: index_value} dict.
    weighting = 'equal' or 'mcap'
    """
    cur.execute("""
        SELECT p.coingecko_id, p.timestamp::date as date, p.price_usd,
               m.market_cap_usd
        FROM price_daily p
        LEFT JOIN marketcap_daily m
          ON p.coingecko_id = m.coingecko_id
          AND p.timestamp::date = m.timestamp::date
        WHERE p.coingecko_id = ANY(%s)
          AND p.timestamp >= %s AND p.timestamp <= %s
          AND p.price_usd > 0
        ORDER BY p.coingecko_id, p.timestamp
    """, (cg_ids, date_from, date_to))
    rows = cur.fetchall()
    if not rows: return {}, []

    # Build per-asset series
    asset_prices = defaultdict(dict)
    asset_mcaps  = defaultdict(dict)
    for row in rows:
        cid = row['coingecko_id']
        d   = str(row['date'])
        asset_prices[cid][d] = float(row['price_usd'])
        if row['market_cap_usd']:
            asset_mcaps[cid][d] = float(row['market_cap_usd'])

    all_dates = sorted(set(d for s in asset_prices.values() for d in s))
    if not all_dates: return {}, []

    # Rebase each asset to 100 at first date
    rebased = {}
    for cid, prices in asset_prices.items():
        sorted_d = sorted(prices)
        first    = prices[sorted_d[0]]
        if first > 0:
            rebased[cid] = {d: prices[d] / first * 100 for d in sorted_d}

    if not rebased: return {}, []

    min_assets = max(1, len(rebased) * 0.5)
    index = {}

    for date in all_dates:
        if weighting == 'mcap':
            # Marketcap-weighted
            vals_w = []
            for cid, series in rebased.items():
                if date in series:
                    mcap = asset_mcaps.get(cid, {}).get(date)
                    if mcap and mcap > 0:
                        vals_w.append((series[date], mcap))
            if len(vals_w) >= min_assets:
                total_w = sum(w for _, w in vals_w)
                index[date] = round(sum(v * w / total_w for v, w in vals_w), 4)
        else:
            # Equal-weighted
            vals = [s[date] for s in rebased.values() if date in s]
            if len(vals) >= min_assets:
                index[date] = round(sum(vals) / len(vals), 4)

    # Rebase index itself to 100
    if index:
        sorted_dates = sorted(index)
        first = index[sorted_dates[0]]
        if first > 0:
            index = {d: round(v / first * 100, 4) for d, v in index.items()}

    return index, sorted(index.keys())


# ── API handlers ──────────────────────────────────────────────────────────────
def handle_assets():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT p.symbol, r.coingecko_name as name
        FROM price_daily p
        LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
        ORDER BY p.symbol
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def handle_price(params):
    symbols   = [s.strip().upper() for s in params.get("symbols",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    if not symbols: return {"error": "no symbols"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s) AND timestamp >= %s AND timestamp <= %s
        ORDER BY symbol, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    data = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in data: data[sym] = {"dates": [], "prices": []}
        data[sym]["dates"].append(str(row["date"]))
        data[sym]["prices"].append(float(row["price_usd"]) if row["price_usd"] else None)

    for sym in data:
        data[sym]["rebased"] = rebase(data[sym]["prices"])
    return data


def handle_sectors():
    return [{"name": n, "count": len(ids)} for n, ids in SECTORS.items()]


def handle_sector_price(params, weighting='equal'):
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    if not sectors: return {"error": "no sectors"}

    conn   = get_conn()
    cur    = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sectors:
        if sector not in SECTORS: continue
        cg_ids = SECTORS[sector]
        if not cg_ids: continue
        index, dates = fetch_sector_index(cur, cg_ids, date_from, date_to, weighting)
        if dates:
            result[sector] = {
                "dates":   dates,
                "rebased": [index[d] for d in dates],
                "count":   len(cg_ids),
            }

    conn.close()
    return result


def handle_intra_corr(params):
    """Rolling pairwise correlation between sector indices."""
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    if len(sectors) < 2: return {"error": "need at least 2 sectors"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Build index series per sector
    sector_series = {}
    all_dates_set = None
    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, 'equal')
        if dates:
            sector_series[sector] = index
            dates_set = set(dates)
            all_dates_set = dates_set if all_dates_set is None else all_dates_set & dates_set

    conn.close()

    if not all_dates_set or len(sector_series) < 2:
        return {"error": "insufficient data"}

    common_dates = sorted(all_dates_set)
    result = {}

    # All pairs
    sec_list = list(sector_series.keys())
    for i in range(len(sec_list)):
        for j in range(i + 1, len(sec_list)):
            a, b   = sec_list[i], sec_list[j]
            xa     = [sector_series[a].get(d) for d in common_dates]
            xb     = [sector_series[b].get(d) for d in common_dates]
            corr   = rolling_corr(xa, xb, window)
            key    = f"{a} / {b}"
            result[key] = {
                "dates":   common_dates,
                "rebased": corr,  # reusing rebased field for chart compat
                "count":   0,
            }

    return result


def handle_btc_corr(params):
    """Rolling correlation of sector indices vs BTC, ETH, Altcoin proxy."""
    sectors   = [s.strip() for s in params.get("sectors",[""])[0].split(",") if s.strip()]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])
    versus    = params.get("versus", ["BTC"])[0].upper()  # BTC, ETH, or ALTS

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch reference asset price
    ref_sym = {"BTC": "BTC", "ETH": "ETH", "ALTS": "BTC"}.get(versus, "BTC")
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (ref_sym, date_from, date_to))
    ref_rows = {str(r["date"]): float(r["price_usd"]) for r in cur.fetchall()}

    result = {}
    for sector in sectors:
        if sector not in SECTORS: continue
        index, dates = fetch_sector_index(cur, SECTORS[sector], date_from, date_to, 'equal')
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
        }

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
        p      = parsed.path
        try:
            if p in ("/", "/index.html"):          self.send_file(BASE_DIR / "index.html")
            elif p == "/api/assets":               self.send_json(handle_assets())
            elif p == "/api/price":                self.send_json(handle_price(params))
            elif p == "/api/sectors":              self.send_json(handle_sectors())
            elif p == "/api/sector-price":         self.send_json(handle_sector_price(params, 'equal'))
            elif p == "/api/sector-mcap":          self.send_json(handle_sector_price(params, 'mcap'))
            elif p == "/api/sector-intra-corr":    self.send_json(handle_intra_corr(params))
            elif p == "/api/sector-btc-corr":      self.send_json(handle_btc_corr(params))
            elif p.startswith("/static/"):         self.send_file(BASE_DIR / p[8:])
            else:
                self.send_response(404); self.end_headers()
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] running on port {PORT}")
    server.serve_forever()
