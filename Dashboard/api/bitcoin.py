"""
api/bitcoin.py — Bitcoin-specific endpoints
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rebase_series, macro_table, rolling_corr
import psycopg2.extras


def _slope(series, window=5):
    """Compute N-day rate of change for a series. Returns same-length array."""
    result = [None] * len(series)
    for i in range(window, len(series)):
        if series[i] is not None and series[i - window] is not None and series[i - window] != 0:
            result[i] = round((series[i] / series[i - window] - 1) * 100, 4)
    return result


def _inflections(slope_series):
    """Find zero-crossings in a slope series. Returns same-length boolean array."""
    result = [False] * len(slope_series)
    for i in range(1, len(slope_series)):
        if slope_series[i] is not None and slope_series[i - 1] is not None:
            if (slope_series[i - 1] < 0 and slope_series[i] >= 0) or \
               (slope_series[i - 1] > 0 and slope_series[i] <= 0):
                result[i] = True
    return result


def handle_btc_epochs(params):
    """BTC x-fold from halving price. Epoch 3/4/5."""
    days_to_show = int(params.get("days", ["1400"])[0])

    HALVINGS = {
        "Epoch 3 (2016)": "2016-07-09",
        "Epoch 4 (2020)": "2020-05-11",
        "Epoch 5 (2024)": "2024-04-20",
    }

    conn   = get_conn()
    cur    = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, halving_date in HALVINGS.items():
        cur.execute(f"""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE symbol = 'BTC'
              AND timestamp::date >= %s::date
              AND timestamp::date <= (%s::date + INTERVAL '{days_to_show} days')
              AND price_usd > 0
            ORDER BY timestamp
        """, (halving_date, halving_date))
        rows = cur.fetchall()
        if not rows: continue

        halving_price = float(rows[0]["price_usd"])
        if halving_price == 0: continue

        days_list, xfold_list = [], []
        hd = datetime.strptime(halving_date, "%Y-%m-%d").date()
        for row in rows:
            d = row["date"]
            day_n = (d - hd).days
            if 0 <= day_n <= days_to_show:
                days_list.append(day_n)
                xfold_list.append(round(float(row["price_usd"]) / halving_price, 6))

        if days_list:
            result[label] = {
                "days":          days_list,
                "values":        xfold_list,
                "halving_price": halving_price,
            }

    conn.close()
    return result


def handle_btc_cycles(params):
    """BTC indexed to 100 at cycle peak, days since peak."""
    days_to_show = int(params.get("days", ["1000"])[0])
    peak_2025    = params.get("peak2025", ["2025-10-06"])[0]

    PEAKS = {
        "2017/18 Bear": "2017-12-17",
        "2021/22 Bear": "2021-11-10",
        "2025 Bear (ongoing)": peak_2025,
    }

    conn   = get_conn()
    cur    = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, peak_date in PEAKS.items():
        cur.execute(f"""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE symbol = 'BTC'
              AND timestamp::date >= %s::date
              AND timestamp::date <= (%s::date + INTERVAL '{days_to_show} days')
              AND price_usd > 0
            ORDER BY timestamp
        """, (peak_date, peak_date))
        rows = cur.fetchall()
        if not rows: continue

        peak_price = float(rows[0]["price_usd"])
        if peak_price == 0: continue

        days_list, indexed_list = [], []
        pd_ = datetime.strptime(peak_date, "%Y-%m-%d").date()
        for row in rows:
            d = row["date"]
            day_n = (d - pd_).days
            if 0 <= day_n <= days_to_show:
                days_list.append(day_n)
                indexed_list.append(round(float(row["price_usd"]) / peak_price * 100, 4))

        if days_list:
            result[label] = {
                "days":       days_list,
                "values":     indexed_list,
                "peak_price": peak_price,
                "peak_date":  peak_date,
            }

    conn.close()
    return result


def handle_btc_rolling(params):
    """BTC rolling N-day return (%)."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["7"])[0])

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "values": [], "window": window}

    dates  = [str(r["date"]) for r in rows]
    prices = [float(r["price_usd"]) for r in rows]

    values = []
    for i, p in enumerate(prices):
        if i < window:
            values.append(None)
        else:
            values.append(round((p / prices[i - window] - 1) * 100, 4))

    return {"dates": dates, "values": values, "window": window}


def handle_btc_gold(params):
    """BTC price + Gold (GLD) price for dual-axis chart."""
    date_from   = params.get("from", ["2020-01-01"])[0]
    date_to     = params.get("to",   ["2099-01-01"])[0]
    granularity = params.get("granularity", ["daily"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()

    tbl = macro_table(granularity)
    cur.execute(f"""
        SELECT timestamp::date as date, close as price
        FROM {tbl}
        WHERE ticker = 'GLD'
          AND timestamp >= %s AND timestamp <= %s
          AND close > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    gold_rows = cur.fetchall()
    conn.close()

    btc_dates  = [str(r["date"]) for r in btc_rows]
    btc_prices = [float(r["price_usd"]) for r in btc_rows]

    gold_map   = {str(r["date"]): float(r["price"]) for r in gold_rows}
    gold_prices = [gold_map.get(d) for d in btc_dates]

    return {
        "dates":       btc_dates,
        "btc_prices":  btc_prices,
        "gold_prices": gold_prices,
    }


def handle_btc_bull(params):
    """BTC indexed to 100 at cycle trough, days since trough."""
    days_to_show = int(params.get("days", ["1000"])[0])

    TROUGHS = {
        "2015 Trough": "2015-08-14",
        "2018 Trough": "2018-12-15",
        "2022 Trough": "2022-11-21",
    }

    conn   = get_conn()
    cur    = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    result = {}

    for label, trough_date in TROUGHS.items():
        cur.execute(f"""
            SELECT timestamp::date as date, price_usd
            FROM price_daily
            WHERE symbol = 'BTC'
              AND timestamp::date >= %s::date
              AND timestamp::date <= (%s::date + INTERVAL '{days_to_show} days')
              AND price_usd > 0
            ORDER BY timestamp
        """, (trough_date, trough_date))
        rows = cur.fetchall()
        if not rows:
            continue

        trough_price = float(rows[0]["price_usd"])
        if trough_price == 0:
            continue

        days_list, indexed_list = [], []
        td_ = datetime.strptime(trough_date, "%Y-%m-%d").date()
        for row in rows:
            d = row["date"]
            day_n = (d - td_).days
            if 0 <= day_n <= days_to_show:
                days_list.append(day_n)
                indexed_list.append(round(float(row["price_usd"]) / trough_price * 100, 4))

        if days_list:
            result[label] = {
                "days":         days_list,
                "values":       indexed_list,
                "trough_price": trough_price,
                "trough_date":  trough_date,
            }

    conn.close()
    return result


def handle_btc_realvol(params):
    """30d/90d/180d rolling annualized vol of BTC log returns."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=190)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "vol_30d": [], "vol_90d": [], "vol_180d": []}

    all_dates  = [str(r["date"]) for r in rows]
    prices     = [float(r["price_usd"]) for r in rows]
    log_rets   = [None] + [
        math.log(prices[i] / prices[i - 1]) if prices[i - 1] > 0 else None
        for i in range(1, len(prices))
    ]

    def rolling_vol(window):
        result = []
        for i in range(len(log_rets)):
            wr = [r for r in log_rets[max(0, i - window + 1):i + 1] if r is not None]
            if len(wr) < window // 2:
                result.append(None)
            else:
                mean = sum(wr) / len(wr)
                std  = math.sqrt(sum((r - mean) ** 2 for r in wr) / len(wr))
                result.append(round(std * math.sqrt(365) * 100, 4))
        return result

    v30  = rolling_vol(30)
    v90  = rolling_vol(90)
    v180 = rolling_vol(180)

    trimmed = [(d, a, b, c) for d, a, b, c in zip(all_dates, v30, v90, v180) if d >= date_from]
    if not trimmed:
        return {"dates": [], "vol_30d": [], "vol_90d": [], "vol_180d": []}

    td, ta, tb, tc_ = zip(*trimmed)
    return {"dates": list(td), "vol_30d": list(ta), "vol_90d": list(tb), "vol_180d": list(tc_)}


def handle_btc_drawdown_ath(params):
    """Continuous drawdown from BTC rolling ATH."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND price_usd > 0
        ORDER BY timestamp
    """)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "values": []}

    all_dates = [str(r["date"]) for r in rows]
    prices    = [float(r["price_usd"]) for r in rows]
    running_max = prices[0]
    dd_all = []
    for p in prices:
        if p > running_max:
            running_max = p
        dd_all.append(round((p / running_max - 1) * 100, 4) if running_max > 0 else 0)

    trimmed = [(d, v) for d, v in zip(all_dates, dd_all) if date_from <= d <= date_to]
    if not trimmed:
        return {"dates": [], "values": []}
    td, tv = zip(*trimmed)
    return {"dates": list(td), "values": list(tv)}


def handle_btc_gold_ratio(params):
    """BTC price / GLD close per day."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()

    cur.execute("""
        SELECT timestamp::date as date, close
        FROM macro_daily
        WHERE ticker = 'GLD'
          AND timestamp >= %s AND timestamp <= %s
          AND close > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    gold_rows = cur.fetchall()
    conn.close()

    gold_map = {str(r["date"]): float(r["close"]) for r in gold_rows}
    dates, values = [], []
    for row in btc_rows:
        d   = str(row["date"])
        gld = gold_map.get(d)
        if gld and gld > 0:
            dates.append(d)
            values.append(round(float(row["price_usd"]) / gld, 6))

    return {"dates": dates, "values": values}


def handle_btc_dominance(params):
    """BTC mcap / total crypto mcap * 100 per day.
    Uses CoinGecko global total market cap (total_marketcap_daily) for accuracy.
    Falls back to summing our 584 assets if the global table is empty."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Primary: use CoinGecko global total market cap
    rows = []
    try:
        cur.execute("""
            SELECT b.timestamp::date as date,
                   b.market_cap_usd as btc_mcap,
                   t.total_mcap_usd as total_mcap
            FROM marketcap_daily b
            JOIN total_marketcap_daily t
              ON b.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (
                SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1
            )
              AND b.timestamp >= %s AND b.timestamp <= %s
              AND b.market_cap_usd > 0
              AND t.total_mcap_usd > 0
            ORDER BY b.timestamp::date
        """, (date_from, date_to))
        rows = cur.fetchall()
    except Exception:
        pass

    # Fallback: sum our tracked assets
    if not rows:
        cur.execute("""
            SELECT b.timestamp::date as date,
                   b.market_cap_usd as btc_mcap,
                   t.total_mcap
            FROM (
                SELECT timestamp::date as date, market_cap_usd
                FROM marketcap_daily
                WHERE coingecko_id IN (SELECT coingecko_id FROM asset_registry WHERE symbol='BTC' LIMIT 1)
                  AND timestamp >= %s AND timestamp <= %s AND market_cap_usd > 0
            ) b
            JOIN (
                SELECT timestamp::date as date, SUM(market_cap_usd) as total_mcap
                FROM marketcap_daily
                WHERE timestamp >= %s AND timestamp <= %s AND market_cap_usd > 0
                GROUP BY timestamp::date
            ) t ON b.date = t.date
            ORDER BY b.date
        """, (date_from, date_to, date_from, date_to))
        rows = cur.fetchall()

    conn.close()

    dates, values = [], []
    for row in rows:
        total = float(row['total_mcap']) if row['total_mcap'] else 0
        btc   = float(row['btc_mcap'])   if row['btc_mcap']   else 0
        if total > 0:
            dates.append(str(row['date']))
            values.append(round(btc / total * 100, 4))

    return {"dates": dates, "values": values}


def handle_btc_funding(params):
    """BTC perpetual 8h funding rate — avg per day + 7d MA."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, AVG(funding_rate) as avg_rate
        FROM funding_8h
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
        GROUP BY timestamp::date
        ORDER BY timestamp::date
    """, (date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "values": [], "ma7": []}

    dates  = [str(r['date']) for r in rows]
    values = [float(r['avg_rate']) if r['avg_rate'] is not None else None for r in rows]

    ma7 = []
    for i in range(len(values)):
        window = [v for v in values[max(0, i - 6):i + 1] if v is not None]
        ma7.append(round(sum(window) / len(window), 8) if window else None)

    return {"dates": dates, "values": values, "ma7": ma7}


def handle_btc_oi(params):
    """Total BTC OI in USD per day (Binance + Bybit) + BTC price overlay.
    Bybit doesn't provide oi_usd, so we compute it: oi_contracts * btc_price."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, exchange, oi_usd, oi_contracts
        FROM open_interest_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp::date
    """, (date_from, date_to))
    oi_rows = cur.fetchall()

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    price_rows = cur.fetchall()
    conn.close()

    price_map = {str(r['date']): float(r['price_usd']) for r in price_rows}

    # Aggregate OI per day: sum across exchanges, converting Bybit contracts to USD
    daily_oi = {}
    for row in oi_rows:
        d = str(row['date'])
        oi_usd = float(row['oi_usd']) if row['oi_usd'] is not None else None

        # If no oi_usd (Bybit), compute from contracts * price
        if oi_usd is None and row['oi_contracts'] is not None:
            btc_price = price_map.get(d)
            if btc_price:
                oi_usd = float(row['oi_contracts']) * btc_price

        if oi_usd is not None:
            daily_oi[d] = daily_oi.get(d, 0) + oi_usd

    dates = sorted(daily_oi.keys())
    oi_values  = [round(daily_oi[d], 2) for d in dates]
    btc_prices = [price_map.get(d) for d in dates]

    return {"dates": dates, "oi_values": oi_values, "btc_prices": btc_prices}


def handle_btc_funding_delta(params):
    """Daily rolling N-day change in avg funding rate (bps) vs N-day BTC price return (%)."""
    import bisect
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["30"])[0])

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=window + 5)).strftime("%Y-%m-%d")
    except Exception:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, AVG(funding_rate) as avg_rate
        FROM funding_8h
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
        GROUP BY timestamp::date
        ORDER BY timestamp::date
    """, (dt_from_ext, date_to))
    funding_rows = cur.fetchall()

    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    price_rows = cur.fetchall()
    conn.close()

    f_dates = [str(r["date"]) for r in funding_rows if r["avg_rate"] is not None]
    f_vals  = [float(r["avg_rate"]) for r in funding_rows if r["avg_rate"] is not None]
    p_dates = [str(r["date"]) for r in price_rows]
    p_vals  = [float(r["price_usd"]) for r in price_rows]

    dates, funding_delta, price_delta = [], [], []
    for i, d in enumerate(p_dates):
        if d < date_from:
            continue
        target = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=window)).strftime("%Y-%m-%d")
        pi = bisect.bisect_right(p_dates, target) - 1
        if pi < 0:
            continue
        p_now  = p_vals[i]
        p_past = p_vals[pi]
        if p_past == 0:
            continue
        fi_now  = bisect.bisect_right(f_dates, d) - 1
        fi_past = bisect.bisect_right(f_dates, target) - 1
        if fi_now < 0 or fi_past < 0:
            continue
        f_now  = f_vals[fi_now]
        f_past = f_vals[fi_past]

        dates.append(d)
        funding_delta.append(round((f_now - f_past) * 10000, 4))
        price_delta.append(round((p_now / p_past - 1) * 100, 4))

    return {"dates": dates, "funding_delta": funding_delta, "price_delta": price_delta, "window": window}


def handle_btc_ma(params):
    """BTC price with moving averages (50d, 200d, and optional custom)."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    custom_ma = params.get("custom", [None])[0]

    # Fetch extra history so MAs are populated from the start of the visible range
    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "price": [], "ma50": [], "ma200": [], "custom_ma": [], "custom_window": None}

    all_dates = [str(r["date"]) for r in rows]
    prices    = [float(r["price_usd"]) for r in rows]

    def sma(window):
        result = []
        for i in range(len(prices)):
            if i < window - 1:
                result.append(None)
            else:
                avg = sum(prices[i - window + 1:i + 1]) / window
                result.append(round(avg, 2))
        return result

    ma50  = sma(50)
    ma200 = sma(200)

    custom_vals = []
    custom_win  = None
    if custom_ma:
        try:
            custom_win = int(custom_ma)
            if 2 <= custom_win <= 365:
                custom_vals = sma(custom_win)
        except:
            pass

    # Trim to requested date range
    trimmed = []
    for i, d in enumerate(all_dates):
        if d >= date_from:
            trimmed.append((d, prices[i], ma50[i], ma200[i],
                            custom_vals[i] if custom_vals else None))

    if not trimmed:
        return {"dates": [], "price": [], "ma50": [], "ma200": [], "custom_ma": [], "custom_window": custom_win}

    td, tp, t50, t200, tc = zip(*trimmed)
    return {
        "dates":         list(td),
        "price":         list(tp),
        "ma50":          list(t50),
        "ma200":         list(t200),
        "custom_ma":     list(tc) if custom_vals else [],
        "custom_window": custom_win,
    }


def handle_btc_200w_floor(params):
    """BTC price with 200-week (1400-day) moving average as a floor indicator.
    Historically BTC has never closed below the 200-week MA in a sustained way —
    it acts as the ultimate macro floor/support level."""
    date_from = params.get("from", ["2015-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    # 200 weeks = 1400 days — need extra history
    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=1410)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = "2012-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "price": [], "ma200w": [], "multiplier": []}

    all_dates = [str(r["date"]) for r in rows]
    prices    = [float(r["price_usd"]) for r in rows]

    # 200-week MA = 1400-day SMA
    window = 1400
    ma200w = []
    for i in range(len(prices)):
        if i < window - 1:
            ma200w.append(None)
        else:
            avg = sum(prices[i - window + 1:i + 1]) / window
            ma200w.append(round(avg, 2))

    # Multiplier: how many x above the 200w MA
    multiplier = []
    for i in range(len(prices)):
        if ma200w[i] and ma200w[i] > 0:
            multiplier.append(round(prices[i] / ma200w[i], 4))
        else:
            multiplier.append(None)

    # Trim to requested range
    trimmed = [(d, p, m, x) for d, p, m, x in zip(all_dates, prices, ma200w, multiplier) if d >= date_from]
    if not trimmed:
        return {"dates": [], "price": [], "ma200w": [], "multiplier": []}

    td, tp, tm, tx = zip(*trimmed)
    return {
        "dates":      list(td),
        "price":      list(tp),
        "ma200w":     list(tm),
        "multiplier": list(tx),
    }


def handle_btc_200d_deviation(params):
    """BTC % deviation from its 200-day moving average.
    Positive = overextended above, negative = oversold below.
    Useful for identifying mean-reversion opportunities."""
    date_from = params.get("from", ["2015-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = "2012-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "deviation": [], "price": [], "ma200": []}

    all_dates = [str(r["date"]) for r in rows]
    prices    = [float(r["price_usd"]) for r in rows]

    window = 200
    ma200 = []
    for i in range(len(prices)):
        if i < window - 1:
            ma200.append(None)
        else:
            avg = sum(prices[i - window + 1:i + 1]) / window
            ma200.append(round(avg, 2))

    deviation = []
    for i in range(len(prices)):
        if ma200[i] and ma200[i] > 0:
            deviation.append(round((prices[i] / ma200[i] - 1) * 100, 2))
        else:
            deviation.append(None)

    trimmed = [(d, dev, p, m) for d, dev, p, m in zip(all_dates, deviation, prices, ma200) if d >= date_from]
    if not trimmed:
        return {"dates": [], "deviation": [], "price": [], "ma200": []}

    td, tdev, tp, tm = zip(*trimmed)

    dev_slope = _slope(list(tdev))
    dev_infl  = _inflections(dev_slope)

    return {
        "dates":     list(td),
        "deviation": list(tdev),
        "price":     list(tp),
        "ma200":     list(tm),
        "dev_slope": dev_slope,
        "dev_inflections": dev_infl,
    }


def handle_btc_ma_gap(params):
    """50d/200d MA gap over time — the % difference between the two MAs.
    Positive = golden cross territory, negative = death cross territory.
    The velocity of this line is what matters most."""
    date_from = params.get("from", ["2015-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=210)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = "2012-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "gap": [], "ma50": [], "ma200": []}

    all_dates = [str(r["date"]) for r in rows]
    prices    = [float(r["price_usd"]) for r in rows]

    def sma(w):
        return [None if i < w - 1 else sum(prices[i-w+1:i+1]) / w for i in range(len(prices))]

    ma50  = sma(50)
    ma200 = sma(200)
    gap   = [round((ma50[i] / ma200[i] - 1) * 100, 4) if ma50[i] and ma200[i] and ma200[i] > 0 else None
             for i in range(len(prices))]

    trimmed = [(d, g, m5, m2) for d, g, m5, m2 in zip(all_dates, gap, ma50, ma200) if d >= date_from]
    if not trimmed:
        return {"dates": [], "gap": [], "ma50": [], "ma200": []}

    td, tg, t5, t2 = zip(*trimmed)

    # Compute slopes and inflections on the trimmed data
    ma50_slope  = _slope(list(t5))
    ma200_slope = _slope(list(t2))
    gap_slope   = _slope(list(tg))
    ma50_infl   = _inflections(ma50_slope)
    ma200_infl  = _inflections(ma200_slope)
    gap_infl    = _inflections(gap_slope)

    return {
        "dates": list(td), "gap": list(tg), "ma50": list(t5), "ma200": list(t2),
        "ma50_slope": ma50_slope, "ma200_slope": ma200_slope, "gap_slope": gap_slope,
        "ma50_inflections": ma50_infl, "ma200_inflections": ma200_infl, "gap_inflections": gap_infl,
    }


def handle_btc_pi_cycle(params):
    """Pi Cycle Top indicator — 111d MA and 2x 350d MA.
    When the 111d MA crosses above the 2x 350d MA, it has historically
    called BTC cycle tops within 3 days. Uses only price data."""
    date_from = params.get("from", ["2015-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=360)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = "2012-01-01"

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "price": [], "ma111": [], "ma350x2": []}

    all_dates = [str(r["date"]) for r in rows]
    prices    = [float(r["price_usd"]) for r in rows]

    def sma(w):
        return [None if i < w - 1 else sum(prices[i-w+1:i+1]) / w for i in range(len(prices))]

    ma111   = sma(111)
    ma350   = sma(350)
    ma350x2 = [round(v * 2, 2) if v is not None else None for v in ma350]

    trimmed = [(d, p, m1, m2) for d, p, m1, m2 in zip(all_dates, prices, ma111, ma350x2) if d >= date_from]
    if not trimmed:
        return {"dates": [], "price": [], "ma111": [], "ma350x2": []}

    td, tp, t1, t2 = zip(*trimmed)
    return {"dates": list(td), "price": list(tp), "ma111": list(t1), "ma350x2": list(t2)}


def handle_btc_mcap(params):
    """BTC market cap over time with milestone levels marked."""
    date_from = params.get("from", ["2015-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT timestamp::date as date, market_cap_usd
        FROM marketcap_daily
        WHERE coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND timestamp >= %s AND timestamp <= %s
          AND market_cap_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "mcap": [], "milestones": []}

    dates = [str(r["date"]) for r in rows]
    mcaps = [float(r["market_cap_usd"]) for r in rows]

    milestones = [
        {"label": "$100B", "value": 1e11},
        {"label": "$500B", "value": 5e11},
        {"label": "$1T",   "value": 1e12},
        {"label": "$2T",   "value": 2e12},
    ]

    return {"dates": dates, "mcap": mcaps, "milestones": milestones}


def handle_btc_rv_iv(params):
    """Realized vol (30d) vs Implied vol (DVOL) — the IV-RV spread.
    When DVOL > RV: market pricing in future risk.
    When RV > DVOL: market is complacent or vol is being realized."""
    date_from = params.get("from", ["2021-03-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    try:
        dt_from_ext = (datetime.strptime(date_from, "%Y-%m-%d") - timedelta(days=40)).strftime("%Y-%m-%d")
    except:
        dt_from_ext = date_from

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # BTC prices for realized vol
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND timestamp <= %s AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    price_rows = cur.fetchall()

    # DVOL
    cur.execute("""
        SELECT timestamp::date as date, close as dvol
        FROM dvol_daily
        WHERE currency = 'BTC' AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp
    """, (date_from, date_to))
    dvol_rows = cur.fetchall()
    conn.close()

    if not price_rows or not dvol_rows:
        return {"dates": [], "rv30": [], "dvol": [], "spread": []}

    # Compute 30d realized vol
    all_dates = [str(r["date"]) for r in price_rows]
    prices    = [float(r["price_usd"]) for r in price_rows]
    log_rets  = [None] + [math.log(prices[i] / prices[i-1]) if prices[i-1] > 0 else None
                          for i in range(1, len(prices))]

    rv_map = {}
    for i in range(29, len(log_rets)):
        wr = [r for r in log_rets[i-29:i+1] if r is not None]
        if len(wr) >= 15:
            mean = sum(wr) / len(wr)
            std  = math.sqrt(sum((r - mean)**2 for r in wr) / len(wr))
            rv_map[all_dates[i]] = round(std * math.sqrt(365) * 100, 2)

    dvol_map = {str(r["date"]): float(r["dvol"]) for r in dvol_rows}

    # Align on DVOL dates (since DVOL starts 2021)
    dates, rv_vals, dvol_vals, spread_vals = [], [], [], []
    for d in sorted(dvol_map.keys()):
        if d < date_from:
            continue
        rv = rv_map.get(d)
        dv = dvol_map.get(d)
        if rv is not None and dv is not None:
            dates.append(d)
            rv_vals.append(rv)
            dvol_vals.append(dv)
            spread_vals.append(round(dv - rv, 2))

    return {"dates": dates, "rv30": rv_vals, "dvol": dvol_vals, "spread": spread_vals}
