"""
BTC Perpetual Funding Rate — 7D EMA Annualised + 7D Hourly Price Return
────────────────────────────────────────────────────────────────────────
Left Y  : 1D annualised funding rate (dotted, light)
          + 7D EMA annualised funding rate (bold)
Right Y : 168h rolling price return on hourly closes (Binance spot)

EMA (span=7) replaces simple rolling mean to reduce lag.
Price return uses pct_change(168) on 1h klines — no resampling.

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

LOOKBACK       = 0.50       # Years to display  (0.25 = 3m, 0.5 = 6m, 1.0 = 1y, 5.0 = full)
EMA_SPAN       = 7          # EMA span in days for funding smoothing
PRICE_WINDOW   = 168        # Rolling return window in hours (168 = 7d × 24h)
SYMBOL_BINANCE = "BTCUSDT"
SYMBOL_BYBIT   = "BTCUSDT"
SYMBOL_PRICE   = "BTCUSDT"  # Binance spot symbol for price

SAVE_DIR       = Path("/home/jasperdemaere/Master_JDM/Exhibit_Library/Perps/Exhibits")

# ══════════════════════════════════════════════════════════════════════════════

# Funding fetch buffer: display window + EMA warmup + 30-day margin
_FETCH_DAYS_FUNDING = int(LOOKBACK * 365) + EMA_SPAN + 30

# Price fetch buffer: same window expressed in hours + PRICE_WINDOW warmup
_FETCH_HOURS_PRICE  = (_FETCH_DAYS_FUNDING + (PRICE_WINDOW // 24)) * 24

# ── PALETTE ───────────────────────────────────────────────────────────────────
C_GREEN  = "#00D64A"
C_BLUE   = "#2471CC"
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


# ── FETCH: FUNDING RATES ──────────────────────────────────────────────────────
def fetch_binance_funding(symbol: str) -> pd.DataFrame:
    """GET https://fapi.binance.com/fapi/v1/fundingRate — paginate backwards."""
    url      = "https://fapi.binance.com/fapi/v1/fundingRate"
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(days=_FETCH_DAYS_FUNDING)
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
        raise RuntimeError("Binance funding: no data.")

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


def fetch_bybit_funding(symbol: str) -> pd.DataFrame:
    """GET https://api.bybit.com/v5/market/funding/history — paginate backwards."""
    url      = "https://api.bybit.com/v5/market/funding/history"
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(days=_FETCH_DAYS_FUNDING)
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
        raise RuntimeError("Bybit funding: no data.")

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


# ── FETCH: SPOT PRICE (BINANCE HOURLY KLINES) ────────────────────────────────
def fetch_binance_price(symbol: str) -> pd.DataFrame:
    """
    GET https://api.binance.com/api/v3/klines  interval=1h
    Paginates backwards to cover _FETCH_HOURS_PRICE of history.
    Computes pct_change(PRICE_WINDOW) on raw hourly closes — no resampling.
    Returns hourly (timestamp, ret_7d) frame.
    """
    url      = "https://api.binance.com/api/v3/klines"
    cutoff   = pd.Timestamp.utcnow() - pd.Timedelta(hours=_FETCH_HOURS_PRICE)
    rows     = []
    end_time = None

    while True:
        params = {"symbol": symbol, "interval": "1h", "limit": 1000}
        if end_time is not None:
            params["endTime"] = end_time

        resp  = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        rows.extend(batch)
        earliest_ms = int(batch[0][0])
        earliest_dt = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc)
        if earliest_dt <= cutoff.to_pydatetime():
            break
        end_time = earliest_ms - 1

    if not rows:
        raise RuntimeError("Binance klines: no data.")

    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_vol", "trades", "taker_base", "taker_quote", "ignore"
    ])
    df["timestamp"] = pd.to_datetime(df["open_time"].astype(int), unit="ms", utc=True)
    df["close"]     = df["close"].astype(float)
    df = (
        df[["timestamp", "close"]]
        .drop_duplicates("timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    # Hourly rolling return — no resampling, preserves full granularity
    df["ret_7d"]    = df["close"].pct_change(PRICE_WINDOW)
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    return df[["timestamp", "ret_7d"]].dropna().reset_index(drop=True)


# ── AGGREGATE: FUNDING ────────────────────────────────────────────────────────
def build_daily_funding(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    1. Combine all exchange 8h ticks
    2. Daily median of raw rates
    3. Annualise: rate × 3 × 365
    4. 7D EMA of annualised rate  (replaces simple rolling mean to reduce lag)
    """
    combined         = pd.concat(dfs, ignore_index=True)
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

    # EMA span=EMA_SPAN — more responsive than rolling mean, half the effective lag
    daily["ema_ann"]  = daily["ann_rate"].ewm(span=EMA_SPAN, adjust=False).mean()

    return daily


def trim_to_lookback(df: pd.DataFrame) -> pd.DataFrame:
    cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(days=int(LOOKBACK * 365))).tz_localize(None)
    col    = "date" if "date" in df.columns else "timestamp"
    return df[df[col] >= cutoff].reset_index(drop=True)


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
def plot(funding: pd.DataFrame, price: pd.DataFrame, exchanges: list[str]):
    vis_f = trim_to_lookback(funding)
    vis_p = trim_to_lookback(price)

    fig, ax_l = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(C_BG)
    style_ax(ax_l)

    # ── right axis — shares x, right spine only
    ax_r = ax_l.twinx()
    ax_r.spines["right"].set_visible(True)
    ax_r.spines["right"].set_color(C_SPINE)
    ax_r.spines["right"].set_linewidth(0.8)
    ax_r.spines["top"].set_visible(False)
    ax_r.spines["left"].set_visible(False)
    ax_r.spines["bottom"].set_visible(False)
    ax_r.tick_params(axis="y", colors=C_SPINE, length=3)
    ax_r.set_facecolor(C_BG)
    ax_r.grid(False)

    dates_f   = vis_f["date"]
    daily_pct = vis_f["ann_rate"] * 100
    ema_pct   = vis_f["ema_ann"]  * 100

    dates_p   = vis_p["timestamp"]
    ret_pct   = vis_p["ret_7d"]   * 100

    # ── LEFT: zero line
    ax_l.axhline(0, color=C_SPINE, linewidth=0.6, zorder=1, alpha=0.4)

    # ── LEFT: fill under EMA line
    ax_l.fill_between(dates_f, ema_pct, 0,
                      where=(vis_f["ema_ann"] >= 0),
                      color=C_GREEN, alpha=0.10, zorder=2)
    ax_l.fill_between(dates_f, ema_pct, 0,
                      where=(vis_f["ema_ann"] < 0),
                      color=C_RED, alpha=0.13, zorder=2)

    # ── LEFT: 1D annualised — dotted, light
    ax_l.plot(dates_f, daily_pct,
              color=C_GREEN, linewidth=0.9, alpha=0.45,
              linestyle=":", zorder=3,
              label="1D annualised funding rate")

    # ── LEFT: 7D EMA annualised — solid, bold
    ax_l.plot(dates_f, ema_pct,
              color=C_GREEN, linewidth=1.8, zorder=4,
              label=f"7D EMA annualised funding rate  (span={EMA_SPAN})")

    # ── RIGHT: 168h rolling price return on hourly closes
    ax_r.plot(dates_p, ret_pct,
              color=C_BLUE, linewidth=1.4, zorder=3, alpha=0.85,
              label=f"168h rolling price return  ({SYMBOL_PRICE})")

    # ── LEFT y-axis
    ax_l.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:+.0f}%"))
    ax_l.set_ylabel("")
    for lbl in ax_l.get_yticklabels():
        _apply_font(lbl, FONT_SAANS, 9)
        lbl.set_color(C_SPINE)

    # ── RIGHT y-axis
    ax_r.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:+.0f}%"))
    ax_r.set_ylabel("")
    for lbl in ax_r.get_yticklabels():
        _apply_font(lbl, FONT_SAANS, 9)
        lbl.set_color(C_SPINE)

    # ── x-axis
    _configure_xaxis(ax_l, LOOKBACK)
    for lbl in ax_l.get_xticklabels(which="major"):
        _apply_font(lbl, FONT_BLENDER, 9)
        lbl.set_color(C_SPINE)

    # ── combined legend
    handles_l, labels_l = ax_l.get_legend_handles_labels()
    handles_r, labels_r = ax_r.get_legend_handles_labels()
    leg = ax_l.legend(
        handles_l + handles_r,
        labels_l  + labels_r,
        loc="upper right", framealpha=0,
    )
    _style_legend(leg)

    # ── source line
    src = fig.text(0.01, -0.03, f"Source: {', '.join(exchanges)}", ha="left")
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
    # ── funding
    funding_fetchers = [
        ("Binance", fetch_binance_funding, SYMBOL_BINANCE),
        ("Bybit",   fetch_bybit_funding,   SYMBOL_BYBIT),
    ]

    dfs       = []
    succeeded = []

    for name, fn, symbol in funding_fetchers:
        print(f"  Fetching {name} funding...", end=" ", flush=True)
        try:
            df = fn(symbol)
            dfs.append(df)
            succeeded.append(name)
            print(f"{len(df):,} rows  "
                  f"[{df['timestamp'].min().date()} → {df['timestamp'].max().date()}]")
        except Exception as e:
            print(f"FAILED — {e}")

    if not dfs:
        print("No funding data fetched. Exiting.")
        sys.exit(1)

    print("  Aggregating funding...")
    funding = build_daily_funding(dfs)

    # ── price
    print(f"  Fetching Binance price ({SYMBOL_PRICE}, 1h klines)...", end=" ", flush=True)
    try:
        price = fetch_binance_price(SYMBOL_PRICE)
        print(f"{len(price):,} hourly rows  "
              f"[{price['timestamp'].min().date()} → {price['timestamp'].max().date()}]")
    except Exception as e:
        print(f"FAILED — {e}")
        sys.exit(1)

    last_f = funding.iloc[-1]
    last_p = price.iloc[-1]
    print(f"  Latest EMA ann. funding : {last_f['ema_ann']*100:+.2f}%")
    print(f"  Latest 168h price return: {last_p['ret_7d']*100:+.2f}%")

    print("  Plotting...")
    plot(funding, price, succeeded)