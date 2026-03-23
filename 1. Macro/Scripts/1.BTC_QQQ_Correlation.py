#!/usr/bin/env python3
"""
Rolling Correlation & Performance Chart
Top panel:    rolling performance (HORIZON days)
Bottom panel: rolling correlation (ROLL_WINDOW days)
"""

import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from pathlib import Path

# ── CHANGE THESE ────────────────────────────────────────
ASSET_1       = "BTC-USD"
ASSET_1_LABEL = "BTC"
ASSET_2       = "QQQ"
ASSET_2_LABEL = "Nasdaq"
HORIZON       = 63       # performance window in days (~3 months)
ROLL_WINDOW   = 7        # correlation rolling window in days
SMOOTH_WINDOW = 60       # avg smoothing overlay in days
LOOKBACK      = 1.5      # total chart lookback in years (decimals fine, e.g. 1.5, 2.5)
# ────────────────────────────────────────────────────────

EXHIBIT_DIR   = Path("/home/jasperdemaere/Master_JDM/ExhibExhibitsit_Library/1. Macro/")

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

# ── PALETTE ──────────────────────────────────────────────
BG             = "#FAFAFA"
AXIS_COLOR     = "#606663"   # Charcoal
SOURCE_COLOR   = "#606663"   # Ash
SUBTITLE_COLOR = "#323935"   # Graphite

C1             = "#00D64A"   # TradFi green
C2             = "#2471CC"
C3             = "#746BE6"
C4             = "#DB33CB"
C5             = "#EC5B5B"

CORR_POS       = "#00D64A"
CORR_NEG       = "#EC5B5B"

# ── DATA ─────────────────────────────────────────────────
def fetch_prices():
    lookback_days = int(LOOKBACK * 365)
    buffer        = max(HORIZON, SMOOTH_WINDOW) + 30
    raw    = yf.download([ASSET_1, ASSET_2], period=f"{lookback_days + buffer}d",
                         interval="1d", auto_adjust=True, progress=False)
    return raw["Close"][[ASSET_1, ASSET_2]].dropna()

def compute_rolling_corr(prices):
    returns = prices.pct_change().dropna()
    return returns[ASSET_1].rolling(ROLL_WINDOW).corr(returns[ASSET_2]).dropna()

def compute_rolling_perf(prices):
    return prices.pct_change(HORIZON).dropna() * 100

def trim_to_lookback(series):
    cutoff = pd.Timestamp.today(tz=series.index.tz) - pd.Timedelta(days=int(LOOKBACK * 365))
    return series[series.index >= cutoff]

# ── TERMINAL SUMMARY ─────────────────────────────────────
def print_summary(corr):
    now_val  = corr.iloc[-1]
    ago_val  = corr.iloc[-31] if len(corr) >= 31 else corr.iloc[0]
    ago_date = (corr.index[-31].strftime("%Y-%m-%d")
                if len(corr) >= 31 else corr.index[0].strftime("%Y-%m-%d"))
    yr_slice = corr.last("365D")
    yr_min, yr_max = yr_slice.min(), yr_slice.max()

    span  = yr_max - yr_min
    p_now = max(0, min(50, int((now_val - yr_min) / span * 50) if span else 0))
    p_ago = max(0, min(50, int((ago_val - yr_min) / span * 50) if span else 0))
    bar   = ["-"] * 51
    bar[0] = "|"; bar[-1] = "|"
    bar[p_ago] = "○"; bar[p_now] = "●"

    print("\n" + "=" * 60)
    print(f"  {ASSET_1_LABEL} / {ASSET_2_LABEL}  {ROLL_WINDOW}-Day Rolling Correlation  ({LOOKBACK:g}y)")
    print("=" * 60)
    print(f"  1-Year range : [{yr_min:+.3f}]  ──  [{yr_max:+.3f}]")
    print(f"  30d ago  ({ago_date}) :  {ago_val:+.3f}")
    print(f"  Current  ({corr.index[-1].strftime('%Y-%m-%d')}) :  {now_val:+.3f}")
    print("\n  " + "".join(bar))
    print("  ○ = 30d ago   ● = today\n")

# ── PLOT ──────────────────────────────────────────────────
def plot(prices, corr, perf):
    corr = trim_to_lookback(corr)
    perf = trim_to_lookback(perf)

    fig = plt.figure(figsize=(12, 5), facecolor=BG)
    gs  = fig.add_gridspec(2, 1, hspace=0.10, height_ratios=[1, 1.6])

    def style_ax(ax, bottom_spine=True):
        ax.set_facecolor(BG)
        ax.tick_params(axis="both", colors=AXIS_COLOR, labelsize=9, length=3)
        ax.spines["left"].set_edgecolor(AXIS_COLOR)
        ax.spines["left"].set_linewidth(0.8)
        ax.spines["bottom"].set_visible(bottom_spine)
        if bottom_spine:
            ax.spines["bottom"].set_edgecolor(AXIS_COLOR)
            ax.spines["bottom"].set_linewidth(0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.yaxis.grid(False)
        ax.xaxis.grid(False)
        ax.set_title("")
        for lbl in ax.get_xticklabels() + ax.get_yticklabels():
            _apply_font(lbl, FONT_SAANS, size=9)
            lbl.set_color(AXIS_COLOR)

    def _style_legend(leg):
        leg.get_frame().set_visible(False)
        for txt in leg.get_texts():
            _apply_font(txt, FONT_SAANS, size=8)
            txt.set_color(SUBTITLE_COLOR)

    # ── Panel 1 (top): rolling performance ───────────────
    ax1 = fig.add_subplot(gs[0])
    style_ax(ax1, bottom_spine=False)

    a1_perf = perf[ASSET_1]
    a2_perf = perf[ASSET_2]

    ax1.plot(a1_perf.index, a1_perf, color=C1, linewidth=1.3,
             label=f"{ASSET_1_LABEL}  {HORIZON}d perf")
    ax1.plot(a2_perf.index, a2_perf, color=C2, linewidth=1.3,
             label=f"{ASSET_2_LABEL}  {HORIZON}d perf")
    ax1.axhline(0, color=AXIS_COLOR, linewidth=0.5, linestyle="--", alpha=0.5)

    ax1.fill_between(a1_perf.index, a1_perf, a2_perf,
                     where=(a1_perf >= a2_perf), alpha=0.08, color=C1, linewidth=0)
    ax1.fill_between(a1_perf.index, a1_perf, a2_perf,
                     where=(a1_perf <  a2_perf), alpha=0.08, color=C2, linewidth=0)

    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.0f}%"))
    ax1.xaxis.set_visible(False)

    ylabel1 = ax1.set_ylabel(f"{HORIZON}d Rolling Return")
    _apply_font(ylabel1, FONT_SAANS, size=9)
    ylabel1.set_color(AXIS_COLOR)

    _style_legend(ax1.legend(fontsize=8, framealpha=0, labelcolor=SUBTITLE_COLOR,
                             loc="upper right"))

    # ── Panel 2 (bottom): correlation ────────────────────
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    style_ax(ax2, bottom_spine=True)

    ax2.fill_between(corr.index, corr, 0,
                     where=(corr >= 0), alpha=0.15, color=CORR_POS, linewidth=0)
    ax2.fill_between(corr.index, corr, 0,
                     where=(corr <  0), alpha=0.15, color=CORR_NEG, linewidth=0)

    ax2.plot(corr.index, corr, color=SUBTITLE_COLOR, linewidth=1.4, zorder=3)
    ax2.axhline(0,    color=AXIS_COLOR, linewidth=0.5, linestyle="--", alpha=0.5)
    ax2.axhline( 0.5, color=CORR_POS,  linewidth=0.6, linestyle=":",  alpha=0.5)
    ax2.axhline(-0.5, color=CORR_NEG,  linewidth=0.6, linestyle=":",  alpha=0.5)

    smooth = corr.rolling(SMOOTH_WINDOW).mean()
    ax2.plot(smooth.index, smooth, color=AXIS_COLOR, linewidth=1.0,
             linestyle="--", alpha=0.7, label=f"{SMOOTH_WINDOW}d avg")

    ax2.set_ylim(-1.05, 1.05)
    ax2.yaxis.set_major_locator(mticker.MultipleLocator(0.25))
    ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.2f"))
    ax2.set_xlim(corr.index[0], corr.index[-1])

    ylabel2 = ax2.set_ylabel("Correlation")
    _apply_font(ylabel2, FONT_SAANS, size=9)
    ylabel2.set_color(AXIS_COLOR)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=30, ha="right")
    for lbl in ax2.get_xticklabels():
        _apply_font(lbl, FONT_BLENDER, size=9)
        lbl.set_color(AXIS_COLOR)

    _style_legend(ax2.legend(fontsize=8, framealpha=0, labelcolor=SUBTITLE_COLOR,
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
    print("Fetching data...")
    prices = fetch_prices()
    corr   = compute_rolling_corr(prices)
    perf   = compute_rolling_perf(prices)
    print_summary(corr)
    plot(prices, corr, perf)

if __name__ == "__main__":
    main()