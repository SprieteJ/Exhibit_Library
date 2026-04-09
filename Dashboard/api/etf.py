"""ETF chart handlers."""
from datetime import datetime, timedelta
from api.shared import get_conn
import psycopg2.extras


def handle_etf_flows(params):
    """Spot ETF net flows: 7-day trailing sum for BTC and ETH."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]
    window    = int(params.get("window", ["7"])[0])

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    result = {}
    for asset in ["BTC", "ETH"]:
        cur.execute("""
            SELECT timestamp::date as date, SUM(flow_usd_m) as total_flow
            FROM etf_flows_daily
            WHERE asset = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY timestamp::date
            ORDER BY timestamp::date
        """, (asset, date_from, date_to))
        rows = cur.fetchall()
        if not rows: continue

        dates = [str(r["date"]) for r in rows]
        daily = [float(r["total_flow"]) if r["total_flow"] else 0 for r in rows]

        # Rolling sum
        rolling = []
        for i in range(len(daily)):
            w = daily[max(0, i - window + 1):i + 1]
            rolling.append(round(sum(w), 2))

        result[asset] = {
            "dates": dates,
            "daily": daily,
            "rolling": rolling,
            "current_daily": daily[-1] if daily else None,
            "current_rolling": rolling[-1] if rolling else None,
        }

    conn.close()
    return {"window": window, "assets": result}


def handle_etf_flows_by_fund(params):
    """Individual fund flows for a given asset (BTC or ETH)."""
    asset     = params.get("asset", ["BTC"])[0]
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT timestamp::date as date, ticker, flow_usd_m
        FROM etf_flows_daily
        WHERE asset = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY ticker, timestamp
    """, (asset, date_from, date_to))
    rows = cur.fetchall()
    conn.close()

    funds = {}
    for r in rows:
        t = r["ticker"]
        if t not in funds: funds[t] = {"dates": [], "flows": []}
        funds[t]["dates"].append(str(r["date"]))
        funds[t]["flows"].append(float(r["flow_usd_m"]) if r["flow_usd_m"] else 0)

    # Compute cumulative for each fund
    for t in funds:
        cumulative = []
        total = 0
        for f in funds[t]["flows"]:
            total += f
            cumulative.append(round(total, 2))
        funds[t]["cumulative"] = cumulative

    return {"asset": asset, "funds": funds}


def handle_etf_flows_weekly(params):
    """Weekly aggregated ETF flows for BTC and ETH."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    result = {}
    for asset in ["BTC", "ETH"]:
        cur.execute("""
            SELECT date_trunc('week', timestamp)::date as week, SUM(flow_usd_m) as total_flow
            FROM etf_flows_daily
            WHERE asset = %s AND timestamp >= %s AND timestamp <= %s
            GROUP BY date_trunc('week', timestamp)::date
            ORDER BY week
        """, (asset, date_from, date_to))
        rows = cur.fetchall()
        if not rows: continue

        result[asset] = {
            "dates": [str(r["week"]) for r in rows],
            "flows": [round(float(r["total_flow"]), 2) if r["total_flow"] else 0 for r in rows],
        }

    conn.close()
    return {"assets": result}


def handle_etf_aum(params):
    """Total AUM per asset from etf_aum_daily — stacked area."""
    date_from = params.get("from", ["2024-01-01"])[0]
    date_to   = params.get("to",   ["2099-01-01"])[0]

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    result = {}
    for asset in ["BTC", "ETH"]:
        cur.execute("""
            SELECT timestamp::date as date, SUM(aum_usd) as total_aum
            FROM etf_aum_daily
            WHERE asset = %s AND timestamp >= %s AND timestamp <= %s AND aum_usd > 0
            GROUP BY timestamp::date
            ORDER BY timestamp::date
        """, (asset, date_from, date_to))
        rows = cur.fetchall()
        if not rows: continue

        result[asset] = {
            "dates": [str(r["date"]) for r in rows],
            "aum": [round(float(r["total_aum"]) / 1e9, 2) for r in rows],
        }

    conn.close()
    return {"assets": result}
