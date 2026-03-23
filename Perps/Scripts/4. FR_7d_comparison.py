"""
Perpetual Funding Rate Comparison — 7D EMA Annualised
───────────────────────────────────────────────────────
Plots the 7D EMA annualised funding rate for multiple tokens on the
same chart. Each token fetched from Binance + Bybit (where available).

No API keys required — all public endpoints.

Install:  pip install requests pandas matplotlib
Run:      python funding_rate_compare.py
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

# Symbols must match Binance/Bybit perpetual naming (e.g. BTCUSDT, ETHUSDT)
TOKENS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

LOOKBACK   = 0.5      # Years to display  (0.25 = 3m, 0.5 = 6m, 1.0 = 1y)
EMA_SPAN   = 7         # EMA span in days

SAVE_DIR   = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/Perps/Exhibits")

# ══════════════════════════════════════════════════════════════════════════════

_FETCH_DAYS = int(LOOKBACK * 365) + EMA_SPAN + 30

# ── PALETTE ───────────────────────────────────────────────────────────────────
# Two or more lines: use palette in order
_PALETTE = ["#00D64A", "#2471CC", "#746BE6", "#DB33CB", "#EC5B5B"]

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


FONT_SAANS   = _find_font("Saans")
FONT_BLENDER = _find_font("Blender")


# ── FETCH ─────────────────────────────────────────────────────────────────────
def fetch_binance_funding(symbol: str) -> pd.DataFrame:
    """GET https://fapi.binance.com/fapi/v1/fundingRate — paginate backwards."""
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
        raise RuntimeError(f"Binance: no funding data for {symbol}.")

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["fundingTime"].astype(int), unit="ms", utc=True)
    df["rate"]      = df["fundingRate"].astype(float)
    return (
        df[["timestamp", "rate"]]
        .drop_duplicates("timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def fetch_bybit_funding(symbol: str) -> pd.DataFrame:
    """GET https://api.bybit.com/v5/market/funding/history — paginate backwards."""
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
            raise RuntimeError(f"Bybit error for {symbol}: {data.get('retMsg')}")

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
        raise RuntimeError(f"Bybit: no funding data for {symbol}.")

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(
        df["fundingRateTimestamp"].astype(int), unit="ms", utc=True
    )
    df["rate"] = df["fundingRate"].astype(float)
    return (
        df[["timestamp", "rate"]]
        .drop_duplicates("timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


# ── AGGREGATE ─────────────────────────────────────────────────────────────────
def build_series(symbol: str) -> pd.Series:
    """
    Fetch from Binance + Bybit, merge, compute daily median,
    annualise, apply 7D EMA. Returns a pd.Series indexed by date.
    Falls back to whichever exchange succeeds if one fails.
    """
    frames = []
    for name, fn in [("Binance", fetch_binance_funding), ("Bybit", fetch_bybit_funding)]:
        try:
            df = fn(symbol)
            frames.append(df)
            print(f"    {name}: {len(df):,} rows")
        except Exception as e:
            print(f"    {name}: FAILED — {e}")

    if not frames:
        raise RuntimeError(f"No funding data for {symbol} from any exchange.")

    combined         = pd.concat(frames, ignore_index=True)
    combined["date"] = combined["timestamp"].dt.date

    daily = (
        combined.groupby("date")["rate"]
        .median()
        .reset_index()
        .rename(columns={"rate": "daily_median"})
    )
    daily["date"]     = pd.to_datetime(daily["date"])
    daily             = daily.sort_values("date").reset_index(drop=True)
    daily["ann_rate"] = daily["daily_median"] * 3 * 365
    daily["ema_ann"]  = daily["ann_rate"].ewm(span=EMA_SPAN, adjust=False).mean()

    return daily.set_index("date")["ema_ann"]


def trim_to_lookback(series: pd.Series) -> pd.Series:
    cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(days=int(LOOKBACK * 365))).tz_localize(None)
    return series[series.index >= cutoff]


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
def plot(series_dict: dict[str, pd.Series]):
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(C_BG)
    style_ax(ax)

    # ── zero line
    ax.axhline(0, color=C_SPINE, linewidth=0.6, zorder=1, alpha=0.4)

    for i, (symbol, series) in enumerate(series_dict.items()):
        color  = _PALETTE[i % len(_PALETTE)]
        vis    = trim_to_lookback(series)
        pct    = vis * 100
        # Clean label: strip USDT suffix
        label  = symbol.replace("USDT", "").replace("BUSD", "")

        ax.plot(vis.index, pct,
                color=color, linewidth=1.6, zorder=3 + i,
                label=f"{label}  7D EMA ann. funding")

    # ── y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:+.0f}%"))
    ax.set_ylabel("")
    for lbl in ax.get_yticklabels():
        _apply_font(lbl, FONT_SAANS, 9)
        lbl.set_color(C_SPINE)

    # ── x-axis
    _configure_xaxis(ax, LOOKBACK)
    for lbl in ax.get_xticklabels(which="major"):
        _apply_font(lbl, FONT_BLENDER, 9)
        lbl.set_color(C_SPINE)

    # ── legend
    leg = ax.legend(loc="upper right", framealpha=0)
    _style_legend(leg)

    # ── source line
    src = fig.text(0.01, -0.03, "Source: Binance, Bybit", ha="left")
    _apply_font(src, FONT_BLENDER, 9)
    src.set_color(C_SOURCE)

    plt.tight_layout()

    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SAVE_DIR / f"{Path(__file__).stem}.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=C_BG)
    print(f"✓  Saved → {out_path}")
    plt.show()


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    series_dict = {}

    for symbol in TOKENS:
        print(f"  Fetching {symbol}...")
        try:
            s = build_series(symbol)
            series_dict[symbol] = s
            print(f"    → {len(s):,} days  |  latest EMA: {s.iloc[-1]*100:+.2f}%")
        except Exception as e:
            print(f"    FAILED — {e}")

    if not series_dict:
        print("No data. Exiting.")
        sys.exit(1)

    print("  Plotting...")
    plot(series_dict)