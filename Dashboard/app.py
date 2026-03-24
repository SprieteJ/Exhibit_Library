#!/usr/bin/env python3
"""
app.py — Wintermute Dashboard
HTTP server + router only. All logic lives in api/*.py
"""

import os, json, urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

from api.assets   import handle_assets, handle_db_status
from api.sector   import (handle_sectors, handle_sector_price, handle_intra_corr,
                           handle_btc_corr, handle_sector_momentum, handle_sector_zscore,
                           handle_sector_bubble, handle_sector_mcap_view)
from api.bitcoin  import handle_btc_epochs, handle_btc_cycles, handle_btc_gold
from api.altcoins import handle_price, handle_alt_scatter
from api.macro    import handle_macro_price

PORT     = int(os.environ.get("PORT", 8080))
BASE_DIR = Path(__file__).parent


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

    def send_file(self, path: Path):
        if not path.exists():
            self.send_response(404); self.end_headers(); return
        mime = {
            ".html": "text/html", ".css": "text/css",
            ".js":   "application/javascript", ".json": "application/json",
            ".png":  "image/png", ".ico": "image/x-icon",
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
            if p in ("/", "/index.html"):
                self.send_file(BASE_DIR / "index.html")

            elif p == "/api/assets":            self.send_json(handle_assets(params))
            elif p == "/api/db-status":         self.send_json(handle_db_status())
            elif p == "/api/price":             self.send_json(handle_price(params))
            elif p == "/api/macro-price":       self.send_json(handle_macro_price(params))

            elif p == "/api/sectors":           self.send_json(handle_sectors())
            elif p == "/api/sector-price":      self.send_json(handle_sector_price(params, "equal"))
            elif p == "/api/sector-mcap":       self.send_json(handle_sector_price(params, "mcap"))
            elif p == "/api/sector-intra-corr": self.send_json(handle_intra_corr(params))
            elif p == "/api/sector-btc-corr":   self.send_json(handle_btc_corr(params))
            elif p == "/api/sector-momentum":   self.send_json(handle_sector_momentum(params))
            elif p == "/api/sector-zscore":     self.send_json(handle_sector_zscore(params))
            elif p == "/api/sector-bubble":     self.send_json(handle_sector_bubble(params))
            elif p == "/api/sector-mcap-view":  self.send_json(handle_sector_mcap_view(params))

            elif p == "/api/btc-epochs":        self.send_json(handle_btc_epochs(params))
            elif p == "/api/btc-cycles":        self.send_json(handle_btc_cycles(params))
            elif p == "/api/btc-gold":          self.send_json(handle_btc_gold(params))

            elif p == "/api/alt-scatter":       self.send_json(handle_alt_scatter(params))

            elif p.startswith("/static/"):
                self.send_file(BASE_DIR / p[8:])
            else:
                self.send_response(404); self.end_headers()

        except Exception as e:
            self.send_json({"error": str(e)}, 500)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] running on port {PORT}")
    server.serve_forever()
