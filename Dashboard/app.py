#!/usr/bin/env python3
"""
app.py — Wintermute Dashboard
HTTP server + router only. All logic lives in api/*.py
"""

import os, json, urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

from api.assets   import handle_assets, handle_db_status, handle_latest_date
from api.sector   import (handle_sectors, handle_sector_price, handle_intra_corr,
                           handle_btc_corr, handle_sector_momentum, handle_sector_zscore,
                           handle_sector_bubble, handle_sector_mcap_view, handle_sector_rrg,
                           handle_sector_dominance, handle_sector_xheatmap, handle_sector_cumulative,
                           handle_sector_vol, handle_sector_drawdown, handle_sector_breadth,
                           handle_sector_funding, handle_sector_oi, handle_sector_sharpe,
                           handle_sector_overview)
from api.bitcoin  import (handle_btc_dominance_ma,
                           handle_btc_epochs, handle_btc_cycles, handle_btc_gold, handle_btc_rolling,
                           handle_btc_bull, handle_btc_realvol, handle_btc_drawdown_ath,
                           handle_btc_gold_ratio, handle_btc_dominance, handle_btc_funding, handle_btc_oi,
                           handle_btc_funding_delta, handle_btc_ma,
                           handle_btc_200w_floor, handle_btc_200d_deviation,
                           handle_btc_ma_gap, handle_btc_pi_cycle, handle_btc_mcap, handle_btc_rv_iv)
from api.altcoins import (handle_price, handle_alt_scatter,
                           handle_alt_altseason, handle_alt_beta, handle_alt_heatmap,
                           handle_alt_ath_drawdown, handle_alt_funding_heatmap,
                           handle_alt_drawdown_ts)
from api.macro    import (handle_macro_price, handle_macro_matrix, handle_macro_dxy_btc,
                           handle_macro_risk, handle_macro_real_yields, handle_macro_stablecoin,
                           handle_macro_sharpe,
                           handle_macro_btc_corr)
from api.crypto_market import handle_total_mcap
from api.etf      import (handle_etf_flows, handle_etf_flows_by_fund)
from api.control_center import handle_control_center
from api.ethereum import (handle_eth_ma, handle_eth_ma_gap, handle_eth_200d_dev,
                           handle_eth_drawdown, handle_eth_mcap, handle_eth_btc_ratio,
                           handle_eth_ma_combined)
from api.alt_market import (handle_alt_mcap, handle_alt_mcap_gap, handle_alt_mcap_dev,
                             handle_dominance_shares, handle_alt_relative_share,
                             handle_btc_alt_ratio, handle_alt_intracorr)

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

            elif p == "/api/latest-date":       self.send_json(handle_latest_date())
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
            elif p == "/api/sector-rrg":        self.send_json(handle_sector_rrg(params))
            elif p == "/api/sector-dominance":  self.send_json(handle_sector_dominance(params))
            elif p == "/api/sector-xheatmap":   self.send_json(handle_sector_xheatmap(params))
            elif p == "/api/sector-cumulative": self.send_json(handle_sector_cumulative(params))
            elif p == "/api/sector-vol":        self.send_json(handle_sector_vol(params))
            elif p == "/api/sector-drawdown":   self.send_json(handle_sector_drawdown(params))
            elif p == "/api/sector-breadth":    self.send_json(handle_sector_breadth(params))
            elif p == "/api/sector-funding":    self.send_json(handle_sector_funding(params))
            elif p == "/api/sector-oi":         self.send_json(handle_sector_oi(params))
            elif p == "/api/sector-sharpe":     self.send_json(handle_sector_sharpe(params))
            elif p == "/api/sector-overview":   self.send_json(handle_sector_overview(params))
            elif p == "/api/sector-overview":   self.send_json(handle_sector_overview(params))

            elif p == "/api/btc-epochs":        self.send_json(handle_btc_epochs(params))
            elif p == "/api/btc-cycles":        self.send_json(handle_btc_cycles(params))
            elif p == "/api/btc-gold":          self.send_json(handle_btc_gold(params))
            elif p == "/api/btc-rolling":       self.send_json(handle_btc_rolling(params))
            elif p == "/api/btc-bull":          self.send_json(handle_btc_bull(params))
            elif p == "/api/btc-realvol":       self.send_json(handle_btc_realvol(params))
            elif p == "/api/btc-drawdown":      self.send_json(handle_btc_drawdown_ath(params))
            elif p == "/api/btc-gold-ratio":    self.send_json(handle_btc_gold_ratio(params))
            elif p == "/api/btc-dominance":     self.send_json(handle_btc_dominance(params))
            elif p == "/api/btc-funding":       self.send_json(handle_btc_funding(params))
            elif p == "/api/btc-oi":            self.send_json(handle_btc_oi(params))
            elif p == "/api/btc-funding-delta": self.send_json(handle_btc_funding_delta(params))
            elif p == "/api/btc-ma":            self.send_json(handle_btc_ma(params))
            elif p == "/api/btc-200w-floor":    self.send_json(handle_btc_200w_floor(params))
            elif p == "/api/btc-200d-dev":      self.send_json(handle_btc_200d_deviation(params))
            elif p == "/api/btc-ma-gap":        self.send_json(handle_btc_ma_gap(params))
            elif p == "/api/btc-pi-cycle":      self.send_json(handle_btc_pi_cycle(params))
            elif p == "/api/btc-mcap":          self.send_json(handle_btc_mcap(params))
            elif p == "/api/btc-dominance-ma": self.send_json(handle_btc_dominance_ma(params))
            elif p == "/api/btc-rv-iv":         self.send_json(handle_btc_rv_iv(params))

            elif p == "/api/alt-scatter":       self.send_json(handle_alt_scatter(params))
            elif p == "/api/alt-altseason":     self.send_json(handle_alt_altseason(params))
            elif p == "/api/alt-beta":          self.send_json(handle_alt_beta(params))
            elif p == "/api/alt-heatmap":       self.send_json(handle_alt_heatmap(params))
            elif p == "/api/alt-ath-drawdown":  self.send_json(handle_alt_ath_drawdown(params))
            elif p == "/api/alt-funding-heatmap": self.send_json(handle_alt_funding_heatmap(params))
            elif p == "/api/alt-drawdown-ts":    self.send_json(handle_alt_drawdown_ts(params))

            elif p == "/api/macro-matrix":      self.send_json(handle_macro_matrix(params))
            elif p == "/api/macro-dxy-btc":     self.send_json(handle_macro_dxy_btc(params))
            elif p == "/api/macro-risk":        self.send_json(handle_macro_risk(params))
            elif p == "/api/macro-real-yields": self.send_json(handle_macro_real_yields(params))
            elif p == "/api/macro-stablecoin":  self.send_json(handle_macro_stablecoin(params))
            elif p == "/api/macro-btc-corr":   self.send_json(handle_macro_btc_corr(params))
            elif p == "/api/macro-sharpe":      self.send_json(handle_macro_sharpe(params))
            elif p == "/api/total-mcap":        self.send_json(handle_total_mcap(params))
            elif p == "/api/etf-flows":        self.send_json(handle_etf_flows(params))
            elif p == "/api/etf-flows-fund":   self.send_json(handle_etf_flows_by_fund(params))
            elif p == "/api/control-center":    self.send_json(handle_control_center(params))

            elif p == "/api/eth-ma":            self.send_json(handle_eth_ma(params))
            elif p == "/api/eth-ma-gap":        self.send_json(handle_eth_ma_gap(params))
            elif p == "/api/eth-200d-dev":      self.send_json(handle_eth_200d_dev(params))
            elif p == "/api/eth-drawdown":      self.send_json(handle_eth_drawdown(params))
            elif p == "/api/eth-mcap":          self.send_json(handle_eth_mcap(params))
            elif p == "/api/eth-btc-ratio":     self.send_json(handle_eth_btc_ratio(params))
            elif p == "/api/eth-ma-combined":  self.send_json(handle_eth_ma_combined(params))

            elif p == "/api/alt-mcap-total":    self.send_json(handle_alt_mcap(params))
            elif p == "/api/alt-mcap-gap":      self.send_json(handle_alt_mcap_gap(params))
            elif p == "/api/alt-mcap-dev":      self.send_json(handle_alt_mcap_dev(params))
            elif p == "/api/dominance-shares":  self.send_json(handle_dominance_shares(params))
            elif p == "/api/alt-rel-share":     self.send_json(handle_alt_relative_share(params))
            elif p == "/api/btc-alt-ratio":     self.send_json(handle_btc_alt_ratio(params))
            elif p == "/api/alt-intracorr":     self.send_json(handle_alt_intracorr(params))

            elif p.startswith("/static/"):
                self.send_file(BASE_DIR / p[1:])
            else:
                self.send_response(404); self.end_headers()

        except Exception as e:
            self.send_json({"error": str(e)}, 500)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[server] running on port {PORT}")
    server.serve_forever()
