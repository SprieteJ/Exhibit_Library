"""
RSI Scatter — Short-term vs structural momentum
X = RSI(LONG)  |  Y = RSI(SHORT)
"""

# ── CHANGE THESE ──────────────────────────────────────────────────────────────
API_KEY        = "CG-jrgUr1nTKsJh6yjWJeLrYaWM"
TOP_N          = 100
FETCH_N        = 200
RSI_SHORT      = 3          # Y-axis — recent / short-term momentum
RSI_LONG       = 14         # X-axis — structural / medium-term momentum
PRICE_DAYS     = 60         # fetch buffer — keep >= RSI_LONG * 3

USER_EXCLUSIONS = set()     # e.g. {"leo", "okb"}

SAVE_DIR = "/home/jasperdemaere/Master_JDM/Exhibit_Library/Top100/Exhibits/"
# ── END CONFIG ────────────────────────────────────────────────────────────────

import os
import time
import requests
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
from pathlib import Path

STABLES = {
    "usdt","usdc","busd","dai","tusd","usdp","usdd","frax","lusd","gusd",
    "fdusd","pyusd","usds","usde","susd","husd","crvusd","mkusd","gho",
    "alusd","dola","mai","mim","usdm","eusd","usd1","usdf","usdg","rlusd",
    "usdy","zusd","ausd","cusd","musd","nusd","ousd","rusd","xusd",
    "eur","eurc","eurs","eurt","jeur","cadc","sgdc","xsgd","bidr","idrt",
    "nzds","brz","tryb",
}
WRAPPED = {
    "wbtc","weth","wbnb","wsol","wmatic","wavax","wftm","wone","wcro",
    "wxrp","wada","wdot","wlink","wltc","steth","wsteth","reth","cbeth",
    "seth2","oseth","frxeth","sfrxeth","ezeth","weeth","rseth","pufeth",
    "meth","sweth","ethx","ankreth","oeth","lido",
}
RWA_SKIP = {
    "xaut","paxg","cache","pmgt",
    "buidl","usyc","ondo","wtgxx","ustb","eutbl","bfusd","jtrsy","usdtb",
    "rain","cc","wlfi","aster","sky","m","figr_heloc",
}
EXCLUDE_ALL = STABLES | WRAPPED | RWA_SKIP | USER_EXCLUSIONS

BG_COLOR  = "#FAFAFA"
CHARCOAL  = "#606663"
GRAPHITE  = "#323935"
BLACK     = "#000000"
DOT_COLOR = "#2471CC"

def _find_font(name):
    for f in fm.findSystemFonts(fontext="ttf"):
        if name.lower() in Path(f).stem.lower():
            return fm.FontProperties(fname=f)
    return fm.FontProperties(family="sans")

def _apply_font(obj, fp, size=12, weight="normal"):
    obj.set_fontproperties(fp)
    obj.set_fontsize(size)
    obj.set_fontweight(weight)

def style_ax(ax):
    ax.set_facecolor(BG_COLOR)
    for spine in ["right", "top"]:
        ax.spines[spine].set_visible(False)
    ax.spines["left"].set_color(CHARCOAL)
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_color(CHARCOAL)
    ax.spines["bottom"].set_linewidth(0.8)
    ax.tick_params(colors=CHARCOAL, length=3, width=0.8)
    ax.grid(False)

def calc_rsi(prices, period):
    prices = np.array(prices, dtype=float)
    if len(prices) < period + 1:
        return None
    deltas = np.diff(prices)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g  = gains[:period].mean()
    avg_l  = losses[:period].mean()
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 1)

BASE = "https://pro-api.coingecko.com/api/v3"

def cg_get(path, params=None):
    params = params or {}
    params["x_cg_pro_api_key"] = API_KEY
    r = requests.get(BASE + path, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_top_markets(n):
    return cg_get("/coins/markets", {
        "vs_currency": "usd",
        "order":       "market_cap_desc",
        "per_page":    n,
        "page":        1,
    })

def fetch_prices(coin_id, days):
    data = cg_get(f"/coins/{coin_id}/market_chart", {
        "vs_currency": "usd",
        "days":        days,
        "interval":    "daily",
    })
    return [p[1] for p in data["prices"]]

def main():
    font_sans    = _find_font("Saans")
    font_blender = _find_font("Blender")

    print(f"Fetching top {FETCH_N} tokens...")
    markets  = fetch_top_markets(FETCH_N)
    filtered = [c for c in markets if c["symbol"].lower() not in EXCLUDE_ALL][:TOP_N]
    print(f"After exclusions: {len(filtered)} tokens\n")

    results = []
    for i, coin in enumerate(filtered):
        print(f"  [{i+1}/{len(filtered)}] {coin['symbol'].upper()}...", end=" ", flush=True)
        try:
            prices = fetch_prices(coin["id"], PRICE_DAYS)
            rsi_s  = calc_rsi(prices, RSI_SHORT)
            rsi_l  = calc_rsi(prices, RSI_LONG)
            mcap   = coin.get("market_cap") or 1e9
            if rsi_s is not None and rsi_l is not None:
                results.append({
                    "symbol": coin["symbol"].upper(),
                    "rsi_s":  rsi_s,
                    "rsi_l":  rsi_l,
                    "mcap":   mcap,
                })
                print(f"RSI({RSI_SHORT})={rsi_s}  RSI({RSI_LONG})={rsi_l}")
            else:
                print("insufficient data")
        except Exception as e:
            print(f"error: {e}")
        if i < len(filtered) - 1:
            time.sleep(0.4)

    if not results:
        print("No results.")
        return

    xs    = np.array([r["rsi_l"] for r in results])
    ys    = np.array([r["rsi_s"] for r in results])
    mcaps = np.array([r["mcap"]  for r in results])
    med_x = float(np.median(xs))
    med_y = float(np.median(ys))

    log_mc = np.log10(np.clip(mcaps, 1e6, None))
    sizes  = ((log_mc - log_mc.min()) / (log_mc.max() - log_mc.min() + 1e-9)) * 220 + 30

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(BG_COLOR)
    style_ax(ax)

    ax.axvline(med_x, color=CHARCOAL, linewidth=0.7, linestyle="--", alpha=0.45)
    ax.axhline(med_y, color=CHARCOAL, linewidth=0.7, linestyle="--", alpha=0.45)

    ax.scatter(xs, ys, s=sizes, color=DOT_COLOR, alpha=0.7, linewidths=0, zorder=3)

    for r, x, y in zip(results, xs, ys):
        lbl = ax.text(x, y + 1.4, r["symbol"], ha="center", va="bottom",
                      color=GRAPHITE, zorder=4)
        _apply_font(lbl, font_blender, size=6.5)

    corners = [
        (98, 98,  "+ structural", "+ momentum", "right", "top"),
        (2,  98,  "- structural", "+ momentum", "left",  "top"),
        (98, 2,   "+ structural", "- momentum", "right", "bottom"),
        (2,  2,   "- structural", "- momentum", "left",  "bottom"),
    ]
    for qx, qy, l1, l2, ha, va in corners:
        dy = -4 if va == "top" else 4
        t1 = ax.text(qx, qy,      l1, ha=ha, va=va, color=CHARCOAL, alpha=0.4, zorder=2)
        t2 = ax.text(qx, qy + dy, l2, ha=ha, va=va, color=CHARCOAL, alpha=0.4, zorder=2)
        _apply_font(t1, font_sans, size=8)
        _apply_font(t2, font_sans, size=8)

    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_xticks([0, 20, 30, 40, 50, 60, 70, 80, 100])
    ax.set_yticks([0, 20, 30, 40, 50, 60, 70, 80, 100])
    for lbl in ax.get_xticklabels():
        _apply_font(lbl, font_blender, size=8)
        lbl.set_color(GRAPHITE)
    for lbl in ax.get_yticklabels():
        _apply_font(lbl, font_sans, size=8)
        lbl.set_color(GRAPHITE)

    xl = ax.set_xlabel(f"RSI({RSI_LONG}) — structural momentum", labelpad=8, color=GRAPHITE)
    yl = ax.set_ylabel(f"RSI({RSI_SHORT}) — recent momentum",    labelpad=8, color=GRAPHITE)
    _apply_font(xl, font_sans, size=9)
    _apply_font(yl, font_sans, size=9)

    plt.tight_layout()

    save_dir = Path(SAVE_DIR)
    save_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(__file__).stem
    out  = save_dir / f"{stem}.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close()
    print(f"\nSaved: {out}")
    os.system(f"xdg-open '{out}'")
    input("Press Enter to exit...")

if __name__ == "__main__":
    main()