"""
api/bitcoin.py — Bitcoin-specific endpoints
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn, rebase_series, macro_table, rolling_corr
import psycopg2.extras


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
        "2025 Bear (ongoing)": peak_2025,  # ATH ~$126k on 2025-10-06
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

    # BTC from price_daily
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
          AND price_usd > 0
        ORDER BY timestamp
    """, (date_from, date_to))
    btc_rows = cur.fetchall()

    # Gold from macro_daily (GLD ETF)
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
    # fetch extra history for the 180d window
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

    # trim to requested date range
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
    # Fetch all BTC history to compute running max
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
    """BTC mcap / total crypto mcap * 100 per day."""
    date_from = params.get("from", ["2020-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT m.timestamp::date as date,
               b.market_cap_usd as btc_mcap,
               total.total_mcap
        FROM marketcap_daily b
        JOIN (
            SELECT timestamp::date as date, SUM(market_cap_usd) as total_mcap
            FROM marketcap_daily
            WHERE timestamp >= %s AND timestamp <= %s
              AND market_cap_usd > 0
            GROUP BY timestamp::date
        ) total ON b.timestamp::date = total.date
        JOIN marketcap_daily m ON m.timestamp::date = b.timestamp::date AND m.coingecko_id = b.coingecko_id
        WHERE b.coingecko_id = (
            SELECT coingecko_id FROM price_daily WHERE symbol = 'BTC' LIMIT 1
        )
          AND b.timestamp >= %s AND b.timestamp <= %s
          AND b.market_cap_usd > 0
        ORDER BY m.timestamp::date
    """, (date_from, date_to, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        # fallback: simpler query
        conn2 = get_conn()
        cur2  = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur2.execute("""
            SELECT b.timestamp::date as date, b.market_cap_usd as btc_mcap,
                   t.total_mcap
            FROM (
                SELECT timestamp::date as date, market_cap_usd
                FROM marketcap_daily
                WHERE coingecko_id IN (SELECT coingecko_id FROM price_daily WHERE symbol='BTC' LIMIT 1)
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
        rows = cur2.fetchall()
        conn2.close()

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
    """Total BTC OI in USD per day + BTC price overlay."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, SUM(oi_usd) as total_oi
        FROM open_interest_daily
        WHERE symbol = 'BTC'
          AND timestamp >= %s AND timestamp <= %s
        GROUP BY timestamp::date
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
    dates, oi_values, btc_prices = [], [], []
    for row in oi_rows:
        d = str(row['date'])
        dates.append(d)
        oi_values.append(float(row['total_oi']) if row['total_oi'] is not None else None)
        btc_prices.append(price_map.get(d))

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

    # Build ordered lists (funding and price may have different dates)
    f_dates = [str(r["date"]) for r in funding_rows if r["avg_rate"] is not None]
    f_vals  = [float(r["avg_rate"]) for r in funding_rows if r["avg_rate"] is not None]
    p_dates = [str(r["date"]) for r in price_rows]
    p_vals  = [float(r["price_usd"]) for r in price_rows]

    # Use price dates as the master series (daily, complete)
    # For each date, look up the value exactly `window` calendar days ago
    dates, funding_delta, price_delta = [], [], []
    for i, d in enumerate(p_dates):
        if d < date_from:
            continue

        # Target date `window` calendar days before d
        target = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=window)).strftime("%Y-%m-%d")

        # Price: find nearest price date <= target
        pi = bisect.bisect_right(p_dates, target) - 1
        if pi < 0:
            continue
        p_now  = p_vals[i]
        p_past = p_vals[pi]
        if p_past == 0:
            continue

        # Funding: find nearest funding date <= d and <= target
        fi_now  = bisect.bisect_right(f_dates, d) - 1
        fi_past = bisect.bisect_right(f_dates, target) - 1
        if fi_now < 0 or fi_past < 0:
            continue
        f_now  = f_vals[fi_now]
        f_past = f_vals[fi_past]

        dates.append(d)
        funding_delta.append(round((f_now - f_past) * 10000, 4))  # bps
        price_delta.append(round((p_now / p_past - 1) * 100, 4))  # %

    return {"dates": dates, "funding_delta": funding_delta, "price_delta": price_delta, "window": window}
