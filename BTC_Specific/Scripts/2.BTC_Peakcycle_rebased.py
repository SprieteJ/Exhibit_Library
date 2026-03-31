#!/usr/bin/env python3
"""
BTC Bear Market Cycles — Price indexed to peak (peak = 100)
2017/18 Bear, 2021/22 Bear, 2025 Bear (ongoing)
X-axis: days since peak | Y-axis: indexed to 100 at peak
Data: CoinGecko Pro API
"""

import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.font_manager as fm
from pathlib import Path
from datetime import datetime, timedelta

# ── CHANGE THESE ────────────────────────────────────────
DAYS_TO_SHOW      = 1000    # days after peak to display
COINGECKO_API_KEY = "CG-jrgUr1nTKsJh6yjWJeLrYaWM"
# ────────────────────────────────────────────────────────

EXHIBIT_DIR = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/1. Macro/Exhibits")
CG_BASE     = "https://pro-api.coingecko.com/api/v3"
HEADERS     = {"x-cg-pro-api-key": COINGECKO_API_KEY}

# ── CYCLE PEAKS ─────────────────────────────────────────
PEAKS = {
    "2017/18 Bear": datetime(2017, 12, 17),
    "2021/22 Bear": datetime(2021, 11, 10),
    "2025 Bear (ongoing)": datetime(2025, 10, 6),
}

# ── FONTS ────────────────────────────────────────────────
def _find_font(name):
    for f in fm.findSystemFonts(fontpaths=None, fontext="ttf"):
        if name.lower() in f.lower():
            return f
    return None

_saans_path   = _find_font("Saans")
_blender_path = _find_font("Blender")
FONT_SAANS    = fm.FontProperties(fname=_saans_path)   if _saans_path   else fm.FontProperties(family="sans")
FONT_BLENDER  = fm.FontProperties(fname=_blender_path) if _blender_path else fm.FontProperties(family="sans")

def _apply_font(obj, font_prop, size=None, weight=None):
    obj.set_fontproperties(font_prop)
    if size:   obj.set_fontsize(size)
    if weight: obj.set_fontweight(weight)

def _style_legend(leg):
    leg.get_frame().set_visible(False)
    for txt in leg.get_texts():
        _apply_font(txt, FONT_SAANS, size=8)
        txt.set_color(SUBTITLE_COLOR)

# ── PALETTE ──────────────────────────────────────────────
BG             = "#FAFAFA"
AXIS_COLOR     = "#606663"
SOURCE_COLOR   = "#606663"
SUBTITLE_COLOR = "#323935"

C1             = "#00D64A"   # TradFi green  — 2017/18
C2             = "#2471CC"   # blue          — 2021/22
C3             = "#746BE6"   # purple        — 2025 (ongoing)

# ── DATA ─────────────────────────────────────────────────
def fetch_cycle(peak_date):
    """
    Fetch daily BTC/USD OHLC from CoinGecko Pro for the window
    [peak_date, peak_date + DAYS_TO_SHOW]. CoinGecko's /coins/{id}/market_chart/range
    returns daily granularity for ranges > 90 days.
    """
    start_ts = int((peak_date - timedelta(days=1)).timestamp())
    end_ts   = int(min(
        peak_date + timedelta(days=DAYS_TO_SHOW + 1),
        datetime.today()
    ).timestamp())

    resp = requests.get(
        f"{CG_BASE}/coins/bitcoin/market_chart/range",
        headers=HEADERS,
        params={
            "vs_currency": "usd",
            "from": start_ts,
            "to":   end_ts,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    prices_raw = data.get("prices", [])
    if not prices_raw:
        return None, None

    # Build series — timestamps are milliseconds
    ts     = pd.to_datetime([p[0] for p in prices_raw], unit="ms", utc=True)
    values = [p[1] for p in prices_raw]
    prices = pd.Series(values, index=ts.tz_localize(None))
    prices = prices[~prices.index.duplicated(keep="last")].sort_index()

    # Closest day to peak
    idx        = prices.index.searchsorted(pd.Timestamp(peak_date))
    idx        = min(idx, len(prices) - 1)
    peak_price = float(prices.iloc[idx])

    # Rebase to 100 at peak
    indexed = (prices / peak_price) * 100

    # Days since peak as integer index
    indexed.index = (indexed.index - pd.Timestamp(peak_date)).days
    indexed = indexed[(indexed.index >= 0) & (indexed.index <= DAYS_TO_SHOW)]

    return indexed, peak_price

# ── PLOT ──────────────────────────────────────────────────
def plot(cycles):
    fig, ax = plt.subplots(figsize=(12, 5), facecolor=BG)

    ax.set_facecolor(BG)
    ax.tick_params(axis="both", colors=AXIS_COLOR, labelsize=9, length=3)
    ax.spines["left"].set_edgecolor(AXIS_COLOR)
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_edgecolor(AXIS_COLOR)
    ax.spines["bottom"].set_linewidth(0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.grid(False)
    ax.xaxis.grid(False)

    colors = [C1, C2, C3]
    widths = [1.2, 1.2, 1.6]

    for (label, series), color, width in zip(cycles.items(), colors, widths):
        ax.plot(series.index, series.values,
                color=color, linewidth=width, linestyle="-",
                label=label, zorder=3)

    ax.axhline(100, color=AXIS_COLOR, linewidth=0.5, linestyle="--", alpha=0.4)

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}"))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())

    xlabel = ax.set_xlabel("Days since peak", color=AXIS_COLOR, fontsize=9)
    _apply_font(xlabel, FONT_SAANS, size=9)
    xlabel.set_color(AXIS_COLOR)

    ylabel = ax.set_ylabel("Price indexed to peak (peak = 100)",
                           color=AXIS_COLOR, fontsize=9)
    _apply_font(ylabel, FONT_SAANS, size=9)
    ylabel.set_color(AXIS_COLOR)

    ax.set_xlim(0, DAYS_TO_SHOW)

    for lbl in ax.get_xticklabels():
        _apply_font(lbl, FONT_BLENDER, size=9)
        lbl.set_color(AXIS_COLOR)
    for lbl in ax.get_yticklabels():
        _apply_font(lbl, FONT_SAANS, size=9)
        lbl.set_color(AXIS_COLOR)

    _style_legend(ax.legend(fontsize=8, framealpha=0, labelcolor=SUBTITLE_COLOR,
                            loc="upper right"))

    src = fig.text(0.01, -0.03, "Source: CoinGecko",
                   color=SOURCE_COLOR, ha="left")
    _apply_font(src, FONT_BLENDER, size=9)

    script_name  = Path(__file__).stem
    EXHIBIT_DIR.mkdir(parents=True, exist_ok=True)
    exhibit_path = EXHIBIT_DIR / f"{script_name}.png"

    plt.tight_layout()
    plt.savefig(exhibit_path, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"  Exhibit saved → {exhibit_path}\n")
    plt.show()

# ── MAIN ─────────────────────────────────────────────────
def main():
    print("Fetching cycle data from CoinGecko...\n")
    cycles = {}
    for label, peak_date in PEAKS.items():
        series, peak_price = fetch_cycle(peak_date)
        if series is not None:
            cycles[label] = series
            print(f"  {label}  |  peak: {peak_date.strftime('%Y-%m-%d')}"
                  f"  |  ${peak_price:,.0f}  |  {len(series)} days")
        else:
            print(f"  {label}  |  no data returned")

    print()
    plot(cycles)

if __name__ == "__main__":
    main()