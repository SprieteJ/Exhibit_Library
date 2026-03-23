"""
Market-Wide Perp/Spot Volume Ratio — Line Chart
─────────────────────────────────────────────────
Fetches top N perp symbols by 24h volume from Binance futures + Bybit linear,
pulls daily klines for each, aggregates, divides by matching Binance spot
volume. Plots the ratio over time as a single line with 7d rolling average.

No API keys required.

Install:  pip install requests pandas matplotlib tqdm
Run:      python perp_spot_ratio.py
"""

import sys
import time
import requests
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from pathlib import Path
from datetime import datetime, timezone

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ══════════════════════════════════════════════════════════════════════════════
# ── CHANGE THESE ──────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

TOP_N         = 30     # Symbols by 24h volume per exchange
LOOKBACK      = 1.0    # Years to display
BINANCE_PAUSE = 0.05   # Seconds between Binance kline calls
BYBIT_PAUSE   = 0.60   # Seconds between Bybit kline calls

SAVE_DIR      = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/Perps/Exhibits")

# ══════════════════════════════════════════════════════════════════════════════

_FETCH_DAYS = int(LOOKBACK * 365) + 7

# ── PALETTE ───────────────────────────────────────────────────────────────────
C_GREEN  = "#00D64A"
C_BLUE   = "#2471CC"
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


def _apply_font(obj, prop, size, weight="normal"):
    try:
        obj.set_fontproperties(prop)
        obj.set_fontsize(size)
        obj.set_fontweight(weight)
    except Exception:
        pass


FONT_SAANS   = _find_font("Saans")
FONT_BLENDER = _find_font("Blender")


# ── SYMBOL DISCOVERY ─────────────────────────────────────────────────────────
def get_binance_top_perps(n: int) -> list[str]:
    resp = requests.get(
        "https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15
    )
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    df = df[df["symbol"].str.endswith("USDT")].copy()
    df["quoteVolume"] = df["quoteVolume"].astype(float)
    symbols = df.sort_values("quoteVolume", ascending=False).head(n)["symbol"].tolist()
    print(f"    Binance top {n}: {', '.join(symbols[:5])} ...")
    return symbols


def get_bybit_top_perps(n: int) -> list[str]:
    resp = requests.get(
        "https://api.bybit.com/v5/market/tickers",
        params={"category": "linear"}, timeout=15
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit tickers: {data.get('retMsg')}")
    df = pd.DataFrame(data["result"]["list"])
    df = df[df["symbol"].str.endswith("USDT")].copy()
    df["turnover24h"] = pd.to_numeric(df["turnover24h"], errors="coerce").fillna(0)
    symbols = df.sort_values("turnover24h", ascending=False).head(n)["symbol"].tolist()
    print(f"    Bybit top {n}:   {', '.join(symbols[:5])} ...")
    return symbols


# ── KLINE FETCHERS ────────────────────────────────────────────────────────────
def _binance_klines(url: str, symbol: str) -> pd.Series:
    """Daily quote volume from any Binance klines endpoint."""
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(days=_FETCH_DAYS)
    rows     = []
    end_time = None

    try:
        while True:
            params = {"symbol": symbol, "interval": "1d", "limit": 1000}
            if end_time is not None:
                params["endTime"] = end_time

            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 30)))
                continue
            resp.raise_for_status()
            batch = resp.json()
            if not batch or isinstance(batch, dict):
                break

            rows.extend(batch)
            earliest_ms = int(batch[0][0])
            if datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc) \
                    <= cutoff.to_pydatetime():
                break
            end_time = earliest_ms - 1
            time.sleep(BINANCE_PAUSE)

    except Exception:
        return pd.Series(dtype=float)

    if not rows:
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_vol","trades","taker_base","taker_quote","ignore"
    ])
    df["date"]      = pd.to_datetime(df["open_time"].astype(int), unit="ms", utc=True)\
                        .dt.tz_localize(None).dt.normalize()
    df["quote_vol"] = df["quote_vol"].astype(float)
    return df.drop_duplicates("date").set_index("date")["quote_vol"]


def _bybit_klines(symbol: str) -> pd.Series:
    """
    Daily turnover (quote volume) from Bybit v5 kline endpoint.
    GET /v5/market/kline  category=linear  interval=D
    Response columns: [startTime, open, high, low, close, volume, turnover]
    Bybit returns newest-first; paginate via 'end' param.
    """
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(days=_FETCH_DAYS)
    rows     = []
    end_ms   = None

    try:
        while True:
            params = {
                "category": "linear",
                "symbol":   symbol,
                "interval": "D",
                "limit":    200,
            }
            if end_ms is not None:
                params["end"] = str(end_ms)

            resp = requests.get(
                "https://api.bybit.com/v5/market/kline",
                params=params, timeout=15
            )
            if resp.status_code == 429:
                time.sleep(30)
                continue
            resp.raise_for_status()
            data = resp.json()

            if data.get("retCode") != 0:
                break

            batch = data["result"]["list"]
            if not batch:
                break

            rows.extend(batch)

            # batch is newest-first; last item = earliest
            earliest_ms = int(batch[-1][0])
            if datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc) \
                    <= cutoff.to_pydatetime():
                break
            end_ms = earliest_ms - 1
            time.sleep(BYBIT_PAUSE)

    except Exception:
        return pd.Series(dtype=float)

    if not rows:
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows, columns=[
        "startTime","open","high","low","close","volume","turnover"
    ])
    df["date"]     = pd.to_datetime(df["startTime"].astype(int), unit="ms", utc=True)\
                       .dt.tz_localize(None).dt.normalize()
    df["turnover"] = df["turnover"].astype(float)
    return df.drop_duplicates("date").set_index("date")["turnover"].sort_index()


# ── AGGREGATE ─────────────────────────────────────────────────────────────────
def _sum_series(fetch_fn, symbols: list[str], label: str) -> pd.Series:
    total  = pd.Series(dtype=float)
    errors = 0
    it     = tqdm(symbols, desc=f"  {label}", ncols=80) if HAS_TQDM else symbols

    for sym in it:
        vol = fetch_fn(sym)
        if vol.empty:
            errors += 1
        else:
            total = vol if total.empty else total.add(vol, fill_value=0)

    print(f"    {label}: {len(symbols)-errors}/{len(symbols)} OK, {errors} failed")
    return total.sort_index()


# ── X-AXIS FORMATTING ─────────────────────────────────────────────────────────
def _configure_xaxis(ax, lookback: float):
    if lookback <= 0.35:
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    elif lookback <= 1.0:
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=0))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    elif lookback <= 2.5:
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1,4,7,10]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    else:
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4,7,10]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))


# ── PLOT ──────────────────────────────────────────────────────────────────────
def plot(ratio: pd.Series, n_binance: int, n_bybit: int):
    # Trim to display lookback
    cutoff  = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=int(LOOKBACK * 365))
    vis     = ratio[ratio.index >= cutoff]
    rolling = vis.rolling(7, min_periods=4).mean()

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(C_BG)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(C_SPINE)
        ax.spines[side].set_linewidth(0.8)
    ax.tick_params(colors=C_SPINE, length=3)
    ax.set_facecolor(C_BG)
    ax.grid(False)

    # Daily ratio — faint fill + thin line
    ax.fill_between(vis.index, vis.values, alpha=0.08, color=C_GREEN, zorder=2)
    ax.plot(vis.index, vis.values,
            color=C_GREEN, linewidth=1.0, alpha=0.5, zorder=3,
            label="Daily ratio")

    # 7d rolling average — solid, prominent
    ax.plot(rolling.index, rolling.values,
            color=C_BLUE, linewidth=1.8, zorder=4,
            label="7d rolling avg")

    # axes
    _configure_xaxis(ax, LOOKBACK)
    for lbl in ax.get_xticklabels(which="major"):
        _apply_font(lbl, FONT_BLENDER, 9)
        lbl.set_color(C_SPINE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1f}×"))
    for lbl in ax.get_yticklabels():
        _apply_font(lbl, FONT_SAANS, 9)
        lbl.set_color(C_SPINE)
    ax.set_ylabel("")

    leg = ax.legend(loc="upper right", framealpha=0)
    leg.get_frame().set_visible(False)
    for text in leg.get_texts():
        _apply_font(text, FONT_SAANS, 8)
        text.set_color(C_LABEL)

    src = fig.text(
        0.01, -0.03,
        f"Source: Binance spot + futures (top {n_binance}), "
        f"Bybit linear (top {n_bybit})  ·  by 24h volume  ·  "
        f"{int(LOOKBACK*12)}m lookback",
        ha="left"
    )
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
    t0 = time.time()

    print("  Fetching top symbols...")
    binance_perp_syms = get_binance_top_perps(TOP_N)
    bybit_perp_syms   = get_bybit_top_perps(TOP_N)

    # Spot pairs = union of base assets from both exchanges
    bases = sorted(set(
        [s.replace("USDT", "") for s in binance_perp_syms] +
        [s.replace("USDT", "") for s in bybit_perp_syms]
    ))
    spot_syms = [f"{b}USDT" for b in bases]
    print(f"    Spot pairs: {len(spot_syms)}")

    print("\n  Binance perp klines...")
    binance_perp = _sum_series(
        lambda s: _binance_klines("https://fapi.binance.com/fapi/v1/klines", s),
        binance_perp_syms, "Binance perps"
    )

    print("\n  Bybit perp klines...")
    bybit_perp = _sum_series(_bybit_klines, bybit_perp_syms, "Bybit perps")

    print("\n  Binance spot klines...")
    binance_spot = _sum_series(
        lambda s: _binance_klines("https://api.binance.com/api/v3/klines", s),
        spot_syms, "Binance spot"
    )

    print("\n  Computing ratio...")
    total_perp = binance_perp.add(bybit_perp, fill_value=0)
    ratio      = (total_perp / binance_spot).dropna()
    ratio      = ratio[ratio > 0]

    last = ratio.iloc[-1]
    print(f"    {len(ratio):,} days  [{ratio.index[0].date()} → {ratio.index[-1].date()}]")
    print(f"    Latest ratio: {last:.3f}×")
    print(f"    Total time  : {(time.time()-t0)/60:.1f} min")

    print("\n  Plotting...")
    plot(ratio, len(binance_perp_syms), len(bybit_perp_syms))