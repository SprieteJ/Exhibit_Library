#!/usr/bin/env python3
"""
Perp / Spot Volume Ratio vs Price
===================================
Binance + Bybit perp/spot ratio, daily.
Price from CoinGecko Pro API, hourly.
"""

import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from pathlib import Path
from datetime import datetime, timezone
import warnings
warnings.filterwarnings("ignore")

# ── CHANGE THESE ───────────────────────────────────────────────────────────────
TOKEN       = "BTC"
CG_ID       = "bitcoin"     # CoinGecko slug: "ethereum", "solana", "ripple", etc.
LOOKBACK    = 0.25          # years as float  (0.25 = 3m, 0.5 = 6m, 1.0 = 1y)
CG_INTERVAL = "hourly"      # price granularity: "hourly" (max 90d) or "daily"
ROLL        = 7             # rolling avg window for ratio (days)
CG_KEY      = "CG-jrgUr1nTKsJh6yjWJeLrYaWM"
# ──────────────────────────────────────────────────────────────────────────────

DAYS = int(LOOKBACK * 365)

# Palette
BG       = "#FAFAFA"
CHARCOAL = "#606663"
GRAPHITE = "#323935"
C1       = "#00D64A"   # price line
C2       = "#2471CC"   # ratio line

# ── FONT HELPERS ───────────────────────────────────────────────────────────────
def _find_font(name):
    for f in fm.findSystemFonts():
        if name.lower() in Path(f).stem.lower():
            return fm.FontProperties(fname=f)
    return fm.FontProperties(family="sans")

SAANS   = _find_font("Saans")
BLENDER = _find_font("Blender")

def _apply_font(obj, fp, size, weight="normal"):
    obj.set_fontproperties(fp)
    obj.set_fontsize(size)
    obj.set_fontweight(weight)

def _style_legend(leg):
    leg.get_frame().set_visible(False)
    for t in leg.get_texts():
        _apply_font(t, SAANS, 8)
        t.set_color(GRAPHITE)

def style_ax(ax, bottom_spine=True):
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    if not bottom_spine:
        ax.spines["bottom"].set_visible(False)
    for spine in ["left", "bottom"]:
        if ax.spines[spine].get_visible():
            ax.spines[spine].set_color(CHARCOAL)
            ax.spines[spine].set_linewidth(0.8)
    ax.tick_params(colors=CHARCOAL, length=3)
    ax.set_facecolor(BG)
    ax.grid(False)

# ── DATA ───────────────────────────────────────────────────────────────────────
def get(url, params=None, headers=None):
    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def cg_price_hourly():
    data = get(
        f"https://pro-api.coingecko.com/api/v3/coins/{CG_ID}/market_chart",
        params={"vs_currency": "usd", "days": DAYS, "interval": CG_INTERVAL},
        headers={"x-cg-pro-api-key": CG_KEY}
    )
    df = pd.DataFrame(data["prices"], columns=["ts", "price"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df.set_index("ts")["price"]

def binance_spot_volume():
    data = get("https://api.binance.com/api/v3/klines",
               {"symbol": f"{TOKEN}USDT", "interval": "1d", "limit": DAYS})
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","tbb","tbq","ignore"])
    df["date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.normalize()
    df["vol"]  = df["quote_vol"].astype(float)
    return df.set_index("date")["vol"].rename("binance_spot")

def binance_perp_volume():
    data = get("https://fapi.binance.com/fapi/v1/klines",
               {"symbol": f"{TOKEN}USDT", "interval": "1d", "limit": DAYS})
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","tbb","tbq","ignore"])
    df["date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.normalize()
    df["vol"]  = df["quote_vol"].astype(float)
    return df.set_index("date")["vol"].rename("binance_perp")

def bybit_spot_volume():
    data = get("https://api.bybit.com/v5/market/kline",
               {"category": "spot", "symbol": f"{TOKEN}USDT",
                "interval": "D", "limit": DAYS})
    rows = data["result"]["list"]
    df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","quote_vol"])
    df["date"] = pd.to_datetime(df["open_time"].astype(float), unit="ms", utc=True).dt.normalize()
    df["vol"]  = df["quote_vol"].astype(float)
    return df.set_index("date")["vol"].rename("bybit_spot")

def bybit_perp_volume():
    data = get("https://api.bybit.com/v5/market/kline",
               {"category": "linear", "symbol": f"{TOKEN}USDT",
                "interval": "D", "limit": DAYS})
    rows = data["result"]["list"]
    df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume","quote_vol"])
    df["date"] = pd.to_datetime(df["open_time"].astype(float), unit="ms", utc=True).dt.normalize()
    df["vol"]  = df["quote_vol"].astype(float)
    return df.set_index("date")["vol"].rename("bybit_perp")

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n  Fetching {TOKEN} data ({DAYS}d)...")

    price = cg_price_hourly()
    vol   = pd.concat([
        binance_spot_volume(), binance_perp_volume(),
        bybit_spot_volume(),   bybit_perp_volume()
    ], axis=1).dropna()

    vol.index         = pd.to_datetime(vol.index)
    vol["total_spot"] = vol["binance_spot"] + vol["bybit_spot"]
    vol["total_perp"] = vol["binance_perp"] + vol["bybit_perp"]
    vol["ratio"]      = vol["total_perp"] / vol["total_spot"]
    vol["ratio_7d"]   = vol["ratio"].rolling(ROLL).mean()

    print(f"  Perp/spot (latest): {vol['ratio'].iloc[-1]:.2f}x")
    print(f"  Perp/spot (7d avg): {vol['ratio_7d'].iloc[-1]:.2f}x\n")

    # ── PLOT ─────────────────────────────────────────────────────────────────
    fig, ax1 = plt.subplots(figsize=(12, 5), facecolor=BG)
    fig.patch.set_facecolor(BG)
    ax2 = ax1.twinx()

    # Price — hourly, left axis
    ax1.plot(price.index, price.values, color=C1, linewidth=1.2, zorder=3,
             label=f"{TOKEN} price")

    # Ratio — daily, right axis
    ax2.plot(vol.index, vol["ratio_7d"], color=C2, linewidth=1.5,
             zorder=2, label=f"Perp/spot {ROLL}d avg")
    ax2.plot(vol.index, vol["ratio"],    color=C2, linewidth=0.5,
             alpha=0.30, zorder=1)

    # Axes styling
    style_ax(ax1)
    style_ax(ax2)
    ax2.spines["left"].set_visible(False)
    ax2.spines["bottom"].set_visible(False)

    # Axis labels
    ylabel1 = ax1.set_ylabel(f"{TOKEN} Price (USD)", fontsize=9, color=GRAPHITE)
    ylabel2 = ax2.set_ylabel("Perp / Spot ratio", fontsize=9, color=GRAPHITE)
    _apply_font(ylabel1, SAANS, 9)
    _apply_font(ylabel2, SAANS, 9)

    ax1.tick_params(axis="y", labelsize=8)
    ax2.tick_params(axis="y", labelsize=8, colors=CHARCOAL)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # x-axis — Blender font, adaptive spacing
    if DAYS <= 30:
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    elif DAYS <= 90:
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
        ax1.xaxis.set_major_locator(mdates.MonthLocator())
    else:
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=0, ha="center")
    for lbl in ax1.xaxis.get_majorticklabels():
        _apply_font(lbl, BLENDER, 8)
    for lbl in ax1.yaxis.get_majorticklabels():
        _apply_font(lbl, SAANS, 8)
    for lbl in ax2.yaxis.get_majorticklabels():
        _apply_font(lbl, SAANS, 8)

    # Legend — upper right, no frame
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    leg = ax1.legend(lines1 + lines2, labels1 + labels2,
                     loc="upper right", framealpha=0)
    _style_legend(leg)

    # Source line
    src = fig.text(0.01, -0.03, "Source: CoinGecko, Binance, Bybit",
                   color=CHARCOAL)
    _apply_font(src, BLENDER, 8)

    plt.tight_layout()
    out = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/Perps/Exhibits") / f"{Path(__file__).stem}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"  Saved → {out}\n")
    plt.show()

if __name__ == "__main__":
    main()