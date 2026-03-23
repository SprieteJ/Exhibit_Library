#!/usr/bin/env python3
"""
BTC Halving Epochs — Performance as X-fold from halving price
Epoch 3 (2016), Epoch 4 (2020), Epoch 5 (2024)
X-axis: days since halving | Y-axis: x-fold log scale
"""

import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.font_manager as fm
from pathlib import Path
from datetime import datetime, timedelta

# ── CHANGE THESE ────────────────────────────────────────
DAYS_TO_SHOW  = 1400    # days after halving to display (~full epoch)
# ────────────────────────────────────────────────────────

EXHIBIT_DIR   = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/BTC_Specific/Exhibits/")

# ── HALVING DATES ────────────────────────────────────────
HALVINGS = {
    "Epoch 3 (2016)": datetime(2016, 7, 9),
    "Epoch 4 (2020)": datetime(2020, 5, 11),
    "Epoch 5 (2024)": datetime(2024, 4, 20),
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
AXIS_COLOR     = "#606663"   # Charcoal
SOURCE_COLOR   = "#606663"   # Ash
SUBTITLE_COLOR = "#323935"   # Graphite

C1             = "#00D64A"   # TradFi green  — Epoch 3
C2             = "#2471CC"   # blue          — Epoch 4
C3             = "#746BE6"   # purple        — Epoch 5 (current)

# ── DATA ─────────────────────────────────────────────────
def fetch_epoch(halving_date):
    start = halving_date - timedelta(days=2)
    end   = min(halving_date + timedelta(days=DAYS_TO_SHOW + 2), datetime.today())

    raw    = yf.download("BTC-USD", start=start, end=end,
                         interval="1d", auto_adjust=True, progress=False)
    if raw.empty:
        return None, None

    prices = raw["Close"].squeeze()
    prices.index = pd.to_datetime(prices.index).tz_localize(None)

    # Closest trading day to halving
    idx           = prices.index.searchsorted(halving_date)
    idx           = min(idx, len(prices) - 1)
    halving_price = float(prices.iloc[idx])

    # X-fold: price / halving_price (1.0 = halving day)
    xfold = prices / halving_price

    # Days since halving as index
    xfold.index = (xfold.index - pd.Timestamp(halving_date)).days
    xfold = xfold[(xfold.index >= 0) & (xfold.index <= DAYS_TO_SHOW)]

    return xfold, halving_price

# ── PLOT ──────────────────────────────────────────────────
def plot(epochs):
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

    for (label, series), color, width in zip(epochs.items(), colors, widths):
        ax.plot(series.index, series.values,
                color=color, linewidth=width, linestyle="-",
                label=label, zorder=3)

    # Reference line at 1x (halving price)
    ax.axhline(1, color=AXIS_COLOR, linewidth=0.5, linestyle="--", alpha=0.4)
    ax.axvline(0, color=AXIS_COLOR, linewidth=0.6, linestyle="--", alpha=0.4)

    # ── LOG SCALE ────────────────────────────────────────
    ax.set_yscale("log")

    # X-fold tick labels: 0.5x, 1x, 2x, 5x, 10x, 20x etc.
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"{x:g}x"
    ))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())

    # Axis labels
    xlabel = ax.set_xlabel("Days since halving", color=AXIS_COLOR, fontsize=9)
    _apply_font(xlabel, FONT_SAANS, size=9)
    xlabel.set_color(AXIS_COLOR)

    ylabel = ax.set_ylabel("X-fold from halving price (log scale)",
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

    # ── SOURCE LINE ──────────────────────────────────────
    src = fig.text(0.01, -0.03, "Source: Yahoo Finance via yfinance",
                   color=SOURCE_COLOR, ha="left")
    _apply_font(src, FONT_BLENDER, size=9)

    # ── SAVE ─────────────────────────────────────────────
    script_name  = Path(__file__).stem
    EXHIBIT_DIR.mkdir(parents=True, exist_ok=True)
    exhibit_path = EXHIBIT_DIR / f"{script_name}.png"

    plt.tight_layout()
    plt.savefig(exhibit_path, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"  Exhibit saved → {exhibit_path}\n")
    plt.show()

# ── MAIN ─────────────────────────────────────────────────
def main():
    print("Fetching epoch data...\n")
    epochs = {}
    for label, halving_date in HALVINGS.items():
        series, halving_price = fetch_epoch(halving_date)
        if series is not None:
            epochs[label] = series
            print(f"  {label}  |  halving price: ${halving_price:,.0f}"
                  f"  |  {len(series)} days of data")

    print()
    plot(epochs)

if __name__ == "__main__":
    main()