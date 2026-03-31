"""
api/crypto_market.py — Crypto market-wide endpoints (total mcap, etc.)
"""
from datetime import datetime, timedelta
from api.shared import get_conn
import psycopg2.extras


def handle_total_mcap(params):
    """Total crypto market cap with 50d and 200d moving averages + optional custom MA."""
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
        SELECT timestamp::date as date, total_mcap_usd
        FROM total_marketcap_daily
        WHERE timestamp >= %s AND timestamp <= %s
          AND total_mcap_usd > 0
        ORDER BY timestamp
    """, (dt_from_ext, date_to))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "mcap": [], "ma50": [], "ma200": [], "custom_ma": [], "custom_window": None}

    all_dates = [str(r["date"]) for r in rows]
    mcaps     = [float(r["total_mcap_usd"]) for r in rows]

    def sma(window):
        result = []
        for i in range(len(mcaps)):
            if i < window - 1:
                result.append(None)
            else:
                avg = sum(mcaps[i - window + 1:i + 1]) / window
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
            trimmed.append((d, mcaps[i], ma50[i], ma200[i],
                            custom_vals[i] if custom_vals else None))

    if not trimmed:
        return {"dates": [], "mcap": [], "ma50": [], "ma200": [], "custom_ma": [], "custom_window": custom_win}

    td, tm, t50, t200, tc = zip(*trimmed)
    return {
        "dates":         list(td),
        "mcap":          list(tm),
        "ma50":          list(t50),
        "ma200":         list(t200),
        "custom_ma":     list(tc) if custom_vals else [],
        "custom_window": custom_win,
    }
