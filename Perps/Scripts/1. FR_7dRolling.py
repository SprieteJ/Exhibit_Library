"""
BTC Perpetual Funding Rate — 7D Rolling Annualised
────────────────────────────────────────────────────
Fetches historical 8h funding rates from Binance + Bybit,
computes 7-day rolling average annualised rate, and plots it
alongside the 1D annualised series.

Formula:  ann_rate = 8h_rate × 3 × 365

No API keys required — all public endpoints.

Install:  pip install requests pandas matplotlib
Run:      python btc_funding_rate.py
"""

import sys
import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from pathlib import Path
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════════════════════════════
# ── CHANGE THESE ──────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

LOOKBACK        = 0.5       # Years to display  (0.25 = 3m, 0.5 = 6m, 1.0 = 1y, 5.0 = full)
ROLLING_WINDOW  = 7          # Days for rolling average
SYMBOL_BINANCE  = "BTCUSDT"
SYMBOL_BYBIT    = "BTCUSDT"

SAVE_DIR        = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/Perps/Exhibits")

# ══════════════════════════════════════════════════════════════════════════════

# Fetch buffer so rolling window is fully populated at start of visible range
_FETCH_DAYS = int(LOOKBACK * 365) + ROLLING_WINDOW + 30

# ── PALETTE ───────────────────────────────────────────────────────────────────
C_GREEN  = "#00D64A"
C_RED    = "#EC5B5B"
C_BG     = "#FAFAFA"
C_SPINE  = "#606663"
C_LABEL  = "#323935"
C_SOURCE = "#606663"


# ── FONT HELPERS ──────────────────────────────────────────────────────────────
def _find_font(name: str) -> fm.FontProperties:
    for f in fm.findSystemFonts():
        try:
            prop = fm.FontProperties(fname=f)
            if name.lower() in prop.get_name().lower():
                return fm.FontProperties(fname=f)
        except Exception:
            continue
    return fm.FontProperties(family="sans")


def _apply_font(obj, prop: fm.FontProperties, size: float, weight: str = "normal"):
    try:
        obj.set_fontproperties(prop)
        obj.set_fontsize(size)
        obj.set_fontweight(weight)
    except Exception:
        pass


def _style_legend(leg):
    leg.get_frame().set_visible(False)
    leg.get_frame().set_alpha(0)
    for text in leg.get_texts():
        _apply_font(text, FONT_SAANS, 8)
        text.set_color(C_LABEL)


def style_ax(ax, bottom_spine: bool = True):
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    if not bottom_spine:
        ax.spines["bottom"].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(C_SPINE)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=C_SPINE, length=3)
    ax.set_facecolor(C_BG)
    ax.grid(False)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)


# Load fonts once
FONT_SAANS   = _find_font("Saans")
FONT_BLENDER = _find_font("Blender")


# ── FETCH ─────────────────────────────────────────────────────────────────────
def fetch_binance(symbol: str) -> pd.DataFrame:
    """
    GET https://fapi.binance.com/fapi/v1/fundingRate
    Max 1000 rows/call — paginate backwards via endTime.
    """
    url      = "https://fapi.binance.com/fapi/v1/fundingRate"
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(days=_FETCH_DAYS)
    rows     = []
    end_time = None

    while True:
        params = {"symbol": symbol, "limit": 1000}
        if end_time is not None:
            params["endTime"] = end_time

        resp  = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            break

        rows.extend(batch)

        earliest_ms = int(batch[0]["fundingTime"])
        earliest_dt = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc)

        if earliest_dt <= cutoff.to_pydatetime():
            break

        end_time = earliest_ms - 1

    if not rows:
        raise RuntimeError("Binance returned no data.")

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["fundingTime"].astype(int), unit="ms", utc=True)
    df["rate"]      = df["fundingRate"].astype(float)
    df["exchange"]  = "Binance"

    return (
        df[["timestamp", "rate", "exchange"]]
        .drop_duplicates("timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def fetch_bybit(symbol: str) -> pd.DataFrame:
    """
    GET https://api.bybit.com/v5/market/funding/history
    Max 200 rows/call — paginate backwards via endTime.
    """
    url      = "https://api.bybit.com/v5/market/funding/history"
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(days=_FETCH_DAYS)
    rows     = []
    end_time = None

    while True:
        params = {"category": "linear", "symbol": symbol, "limit": 200}
        if end_time is not None:
            params["endTime"] = str(end_time)

        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {data.get('retMsg')}")

        batch = data["result"]["list"]
        if not batch:
            break

        rows.extend(batch)

        earliest_ms = int(batch[-1]["fundingRateTimestamp"])
        earliest_dt = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc)

        if earliest_dt <= cutoff.to_pydatetime():
            break

        end_time = earliest_ms - 1

    if not rows:
        raise RuntimeError("Bybit returned no data.")

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(
        df["fundingRateTimestamp"].astype(int), unit="ms", utc=True
    )
    df["rate"]     = df["fundingRate"].astype(float)
    df["exchange"] = "Bybit"

    return (
        df[["timestamp", "rate", "exchange"]]
        .drop_duplicates("timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


# ── AGGREGATE ─────────────────────────────────────────────────────────────────
def build_daily(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    combined         = pd.concat(dfs, ignore_index=True)
    combined["date"] = combined["timestamp"].dt.date

    daily = (
        combined.groupby("date")["rate"]
        .median()
        .reset_index()
        .rename(columns={"rate": "daily_median"})
    )
    daily["date"]        = pd.to_datetime(daily["date"])
    daily                = daily.sort_values("date").reset_index(drop=True)
    daily["ann_rate"]    = daily["daily_median"] * 3 * 365
    daily["roll_7d_ann"] = daily["ann_rate"].rolling(ROLLING_WINDOW, min_periods=1).mean()

    return daily


def trim_to_lookback(daily: pd.DataFrame) -> pd.DataFrame:
    cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(days=int(LOOKBACK * 365))).tz_localize(None)
    return daily[daily["date"] >= cutoff].reset_index(drop=True)


# ── X-AXIS FORMATTING ─────────────────────────────────────────────────────────
def _configure_xaxis(ax, lookback: float):
    if lookback <= 0.35:
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
        ax.xaxis.set_minor_locator(mdates.DayLocator(interval=7))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    elif lookback <= 1.0:
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=0))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    elif lookback <= 2.5:
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    else:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4, 7, 10]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.xaxis.set_minor_formatter(mdates.DateFormatter("%b"))
        for lbl in ax.get_xticklabels(which="minor"):
            _apply_font(lbl, FONT_BLENDER, 8)
            lbl.set_color(C_SPINE)


# ── PLOT ──────────────────────────────────────────────────────────────────────
def plot(daily: pd.DataFrame, exchanges: list[str]):
    visible   = trim_to_lookback(daily)
    dates     = visible["date"]
    daily_pct = visible["ann_rate"]    * 100
    roll_pct  = visible["roll_7d_ann"] * 100

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(C_BG)
    style_ax(ax)

    # ── zero line
    ax.axhline(0, color=C_SPINE, linewidth=0.6, zorder=1, alpha=0.4)

    # ── fills under 7D rolling line
    ax.fill_between(dates, roll_pct, 0,
                    where=(visible["roll_7d_ann"] >= 0),
                    color=C_GREEN, alpha=0.10, zorder=2)
    ax.fill_between(dates, roll_pct, 0,
                    where=(visible["roll_7d_ann"] < 0),
                    color=C_RED, alpha=0.13, zorder=2)

    # ── 1D annualised — single continuous curve
    ax.plot(dates, daily_pct,
            color=C_GREEN, linewidth=0.9, alpha=0.40, zorder=3,
            label="1D annualised funding rate")

    # ── 7D rolling — single continuous curve
    ax.plot(dates, roll_pct,
            color=C_GREEN, linewidth=1.8, zorder=4,
            label="7D rolling annualised funding rate")

    # ── x-axis
    _configure_xaxis(ax, LOOKBACK)
    for lbl in ax.get_xticklabels(which="major"):
        _apply_font(lbl, FONT_BLENDER, 9)
        lbl.set_color(C_SPINE)

    # ── y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:+.0f}%"))
    ax.set_ylabel("")
    for lbl in ax.get_yticklabels():
        _apply_font(lbl, FONT_SAANS, 9)
        lbl.set_color(C_SPINE)

    # ── legend
    leg = ax.legend(loc="upper right", framealpha=0)
    _style_legend(leg)

    # ── source line
    src = fig.text(0.01, -0.03, f"Source: {', '.join(exchanges)}", ha="left")
    _apply_font(src, FONT_BLENDER, 9)
    src.set_color(C_SOURCE)

    plt.tight_layout()

    # ── save
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SAVE_DIR / f"{Path(__file__).stem}.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=C_BG)
    print(f"✓  Saved → {out_path}")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    fetchers = [
        ("Binance", fetch_binance, SYMBOL_BINANCE),
        ("Bybit",   fetch_bybit,   SYMBOL_BYBIT),
    ]

    dfs       = []
    succeeded = []

    for name, fn, symbol in fetchers:
        print(f"  Fetching {name}...", end=" ", flush=True)
        try:
            df = fn(symbol)
            dfs.append(df)
            succeeded.append(name)
            print(f"{len(df):,} rows  "
                  f"[{df['timestamp'].min().date()} → {df['timestamp'].max().date()}]")
        except Exception as e:
            print(f"FAILED — {e}")

    if not dfs:
        print("No data fetched. Exiting.")
        sys.exit(1)

    print("  Aggregating...")
    daily = build_daily(dfs)

    last = daily.iloc[-1]
    print(f"  {len(daily):,} days  |  latest 7D ann. rate: {last['roll_7d_ann']*100:+.2f}%")

    print("  Plotting...")
    plot(daily, succeeded)