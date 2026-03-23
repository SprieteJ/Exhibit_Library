#!/usr/bin/env python3
"""
app.py
------
Lightweight HTTP server for the Wintermute dashboard.
Uses only stdlib + psycopg2 (no Flask, no FastAPI).

Endpoints:
  GET /                  → serves index.html
  GET /api/assets        → list of all assets (symbol + name)
  GET /api/price         → rebased daily price for given symbols + date range
  GET /api/sectors       → list of sector names + asset counts
  GET /api/sector-price  → equal-weighted rebased price per sector + date range

Environment variables (set in Railway):
  DATABASE_URL           → PostgreSQL connection string
"""

import os
import json
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────
PORT         = int(os.environ.get("PORT", 8080))
DB_URL       = os.environ.get("DATABASE_URL", "")
BASE_DIR     = Path(__file__).parent
SECTORS_FILE = BASE_DIR / "sectors.json"

# ── Load sectors ──────────────────────────────────────────────────────────────
def load_sectors() -> dict:
    if SECTORS_FILE.exists():
        with open(SECTORS_FILE) as f:
            return json.load(f)
    return {}

SECTORS = load_sectors()

# ── DB connection ─────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DB_URL)


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


def handle_price(params: dict):
    symbols   = params.get("symbols", [""])[0].split(",")
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    symbols   = [s.strip().upper() for s in symbols if s.strip()]

    if not symbols:
        return {"error": "no symbols provided"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT symbol, timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = ANY(%s)
          AND timestamp >= %s
          AND timestamp <= %s
        ORDER BY symbol, timestamp
    """, (symbols, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    data = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in data:
            data[sym] = {"dates": [], "prices": []}
        data[sym]["dates"].append(str(row["date"]))
        data[sym]["prices"].append(float(row["price_usd"]) if row["price_usd"] else None)

    for sym in data:
        prices = data[sym]["prices"]
        first  = next((p for p in prices if p is not None), None)
        if first:
            data[sym]["rebased"] = [
                round(p / first * 100, 4) if p is not None else None
                for p in prices
            ]
        else:
            data[sym]["rebased"] = prices

    return data


def handle_sectors():
    return [
        {"name": name, "count": len(ids)}
        for name, ids in SECTORS.items()
    ]


def handle_sector_price(params: dict):
    sector_names = params.get("sectors", [""])[0].split(",")
    date_from    = params.get("from", ["2024-01-01"])[0]
    date_to      = params.get("to",   ["2099-01-01"])[0]
    sector_names = [s.strip() for s in sector_names if s.strip()]

    if not sector_names:
        return {"error": "no sectors provided"}

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for sector in sector_names:
        if sector not in SECTORS:
            continue
        cg_ids = SECTORS[sector]
        if not cg_ids:
            continue

        cur.execute("""
            SELECT coingecko_id, timestamp::date as date, price_usd
            FROM price_daily
            WHERE coingecko_id = ANY(%s)
              AND timestamp >= %s
              AND timestamp <= %s
              AND price_usd IS NOT NULL
              AND price_usd > 0
            ORDER BY coingecko_id, timestamp
        """, (cg_ids, date_from, date_to))
        rows = cur.fetchall()

        if not rows:
            continue

        asset_series = {}
        for row in rows:
            cid = row["coingecko_id"]
            if cid not in asset_series:
                asset_series[cid] = {}
            asset_series[cid][str(row["date"])] = float(row["price_usd"])

        all_dates = sorted(set(d for s in asset_series.values() for d in s))
        if not all_dates:
            continue

        rebased_series = {}
        for cid, prices in asset_series.items():
            sorted_dates = sorted(prices.keys())
            first_price  = prices[sorted_dates[0]]
            if first_price > 0:
                rebased_series[cid] = {
                    d: round(prices[d] / first_price * 100, 4)
                    for d in sorted_dates
                }

        if not rebased_series:
            continue

        min_assets   = max(1, len(rebased_series) * 0.5)
        index_dates  = []
        index_values = []

        for date in all_dates:
            vals = [s[date] for s in rebased_series.values() if date in s]
            if len(vals) >= min_assets:
                index_dates.append(date)
                index_values.append(round(sum(vals) / len(vals), 4))

        if not index_dates:
            continue

        first = index_values[0]
        result[sector] = {
            "dates":   index_dates,
            "rebased": [round(v / first * 100, 4) for v in index_values],
            "count":   len(rebased_series),
        }

    conn.close()
    return result


# ── Request handler ───────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: Path):
        if not path.exists():
            self.send_response(404)
            self.end_headers()
            return
        ext  = path.suffix.lower()
        mime = {
            ".html": "text/html",
            ".css":  "text/css",
            ".js":   "application/javascript",
            ".json": "application/json",
            ".png":  "image/png",
            ".ico":  "image/x-icon",
        }.get(ext, "application/octet-stream")
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        path   = parsed.path

        try:
            if path in ("/", "/index.html"):
                self.send_file(BASE_DIR / "index.html")
            elif path == "/api/assets":
                self.send_json(handle_assets())
            elif path == "/api/price":
                self.send_json(handle_price(params))
            elif path == "/api/sectors":
                self.send_json(handle_sectors())
            elif path == "/api/sector-price":
                self.send_json(handle_sector_price(params))
            elif path.startswith("/static/"):
                self.send_file(BASE_DIR / path[8:])
            else:
                self.send_response(404)
                self.end_headers()
        except Exception as e:
            self.send_json({"error": str(e)}, 500)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] running on port {PORT}")
    server.serve_forever()
