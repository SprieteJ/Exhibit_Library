"""
api/bitcoin.py — Bitcoin-specific endpoints
"""
from datetime import datetime
from .shared import get_conn, rebase_series, macro_table
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
