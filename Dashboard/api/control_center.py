"""
api/control_center.py — Rule-based signal matrix
Each chart can have multiple rules. Each rule is green (look at this) or grey (nothing to see).
Structure: category > chart > rules
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn
import psycopg2.extras


def _sma(prices, window):
    return [None if i < window - 1 else sum(prices[i - window + 1:i + 1]) / window
            for i in range(len(prices))]


def _slope(series, window=5):
    result = [None] * len(series)
    for i in range(window, len(series)):
        if series[i] is not None and series[i - window] is not None and series[i - window] != 0:
            result[i] = round((series[i] / series[i - window] - 1) * 100, 4)
    return result


def _recent_zero_cross(slope_series, lookback=7):
    for i in range(-1, max(-lookback - 1, -len(slope_series)), -1):
        idx = len(slope_series) + i
        if idx < 1: continue
        s_now, s_prev = slope_series[idx], slope_series[idx - 1]
        if s_now is not None and s_prev is not None:
            if s_prev < 0 and s_now >= 0: return True, "up"
            if s_prev > 0 and s_now <= 0: return True, "down"
    return False, None


def _last_valid(series):
    for v in reversed(series):
        if v is not None: return v
    return None


def _fetch_prices(cur, symbol, days_back=1500):
    dt_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT timestamp::date as date, price_usd FROM price_daily
        WHERE symbol = %s AND timestamp >= %s AND price_usd > 0 ORDER BY timestamp
    """, (symbol, dt_from))
    rows = cur.fetchall()
    return [str(r['date']) for r in rows], [float(r['price_usd']) for r in rows]


def _rules_ma_gap(prices, category, tab, prefix):
    if len(prices) < 200: return []
    ma50 = _sma(prices, 50)
    ma200 = _sma(prices, 200)
    m50, m200 = ma50[-1], ma200[-1]
    if not m50 or not m200 or m200 == 0: return []
    gap = (m50 / m200 - 1) * 100
    gap_series = [(ma50[i] / ma200[i] - 1) * 100 if ma50[i] and ma200[i] and ma200[i] > 0 else None for i in range(len(prices))]
    sl50, sl200, sl_gap = _slope(ma50), _slope(ma200), _slope(gap_series)
    pos = "above" if gap > 0 else "below"

    near = abs(gap) < 3
    cross_type = "golden cross" if gap > 0 and near else ("death cross" if gap < 0 and near else "")
    crossed_50, dir_50 = _recent_zero_cross(sl50)
    sl50_now = _last_valid(sl50)
    near_zero_50 = sl50_now is not None and abs(sl50_now) < 0.3
    crossed_200, dir_200 = _recent_zero_cross(sl200)
    sl200_now = _last_valid(sl200)
    near_zero_200 = sl200_now is not None and abs(sl200_now) < 0.15
    crossed_gap, dir_gap = _recent_zero_cross(sl_gap)
    sl_gap_now = _last_valid(sl_gap)

    key = f"{prefix.lower()}-ma-gap" if prefix != "Alt" else "am-mcap-gap"
    rules = [
        {"name": "Near crossing", "active": near,
         "detail": f"Gap at {gap:+.1f}% — {cross_type}" if near else f"Gap at {gap:+.1f}%, 50d {pos} 200d",
         "context": "When the gap approaches zero a golden or death cross is imminent."},
        {"name": "50d MA inflecting", "active": crossed_50 or near_zero_50,
         "detail": f"50d just turned {'up' if dir_50 == 'up' else 'down'}" if crossed_50 else (f"50d slope flattening ({sl50_now:+.2f}%/5d)" if near_zero_50 else f"50d slope {sl50_now:+.2f}%/5d" if sl50_now else "—"),
         "context": "Short-term momentum shift. The 50d turning is the first sign of a trend change."},
        {"name": "200d MA inflecting", "active": crossed_200 or near_zero_200,
         "detail": f"200d just turned {'up' if dir_200 == 'up' else 'down'}" if crossed_200 else (f"200d slope flattening ({sl200_now:+.3f}%/5d)" if near_zero_200 else f"200d slope {sl200_now:+.3f}%/5d" if sl200_now else "—"),
         "context": "The slowest-moving signal. When the 200d turns, institutions notice."},
        {"name": "Gap direction reversing", "active": crossed_gap,
         "detail": f"Gap now {'widening' if dir_gap == 'up' else 'narrowing'} after reversal" if crossed_gap else (f"Gap {'widening' if sl_gap_now and sl_gap_now > 0 else 'narrowing'} ({sl_gap_now:+.2f}%/5d)" if sl_gap_now else "—"),
         "context": "Gap re-widening = trend re-accelerating. Narrowing = momentum fading, cross risk rising."},
    ]
    return [{"category": category, "chart_name": f"{prefix} 50d and 200d Gap", "chart_tab": tab, "chart_key": key, "rules": rules}]


def _rules_200w_deviation(prices, category, tab, prefix):
    if len(prices) < 1400: return []
    ma = _sma(prices, 1400)
    m = ma[-1]
    if not m or m == 0: return []
    dev = (prices[-1] / m - 1) * 100
    dev_series = [(prices[i] / ma[i] - 1) * 100 if ma[i] and ma[i] > 0 else None for i in range(len(prices))]
    sl = _slope(dev_series)
    crossed, direction = _recent_zero_cross(sl)
    sl_now = _last_valid(sl)
    key = f"{prefix.lower()}-200d-dev" if prefix != "Alt" else "am-mcap-dev"

    rules = [
        {"name": "Near floor", "active": dev < 30,
         "detail": f"{dev:+.0f}% from 200-week MA",
         "context": "Approaching the 200-week MA marks generational buy zones. BTC has never closed below it."},
        {"name": "Extreme extension", "active": dev > 300,
         "detail": f"{dev:+.0f}% above 200-week MA",
         "context": ">300% above the 200-week MA has historically marked cycle tops."},
        {"name": "Deviation inflecting", "active": crossed,
         "detail": f"Deviation turning {'up' if direction == 'up' else 'down'}" if crossed else (f"Deviation slope {sl_now:+.2f}%/5d" if sl_now else "—"),
         "context": "Deviation changing direction signals a shift in cycle momentum."},
    ]
    return [{"category": category, "chart_name": f"{prefix} 200-Week Deviation", "chart_tab": tab, "chart_key": key, "rules": rules}]


def _rules_drawdown(prices, category, tab, prefix):
    if not prices: return []
    rm = max(prices)
    dd = (prices[-1] / rm - 1) * 100
    rm_7d = max(prices[:-7]) if len(prices) > 7 else rm
    dd_7d = (prices[-8] / rm_7d - 1) * 100 if len(prices) > 8 else dd
    accel = dd < dd_7d - 5
    key = f"{prefix.lower()}-drawdown" if prefix != "Alt" else "am-mcap-dev"

    rules = [
        {"name": "Bear territory", "active": dd < -20, "detail": f"{dd:.1f}% from ATH",
         "context": ">20% drawdown historically marks bear territory."},
        {"name": "Near ATH", "active": dd > -3,
         "detail": f"{dd:.1f}% from ATH" if dd < 0 else "At all-time high",
         "context": "Within 3% of ATH. Breakouts attract momentum flows."},
        {"name": "Drawdown accelerating", "active": accel,
         "detail": f"Dropped {abs(dd - dd_7d):.1f}pp in 7d (now {dd:.1f}%)" if accel else f"Stable at {dd:.1f}%",
         "context": "Drawdown deepening >5pp in a week signals panic or forced selling."},
    ]
    return [{"category": category, "chart_name": f"{prefix} Drawdown from ATH", "chart_tab": tab, "chart_key": key, "rules": rules}]


def _rules_volatility(cur, prices):
    rules_rv, rules_iv = [], []
    rv30 = None
    if len(prices) >= 35:
        log_rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
        if len(log_rets) >= 30:
            rets = log_rets[-30:]
            mean = sum(rets) / len(rets)
            std = math.sqrt(sum((r - mean)**2 for r in rets) / len(rets))
            rv30 = std * math.sqrt(365) * 100
            rules_rv = [
                {"name": "Unusually low vol", "active": rv30 < 30, "detail": f"30d RV at {rv30:.1f}%",
                 "context": "Vol below 30% often precedes explosive moves. The calm before the storm."},
                {"name": "Crisis-level vol", "active": rv30 > 80, "detail": f"30d RV at {rv30:.1f}%",
                 "context": ">80% annualized vol = crisis territory."},
            ]
    try:
        cur.execute("SELECT close FROM dvol_daily WHERE currency = 'BTC' ORDER BY timestamp DESC LIMIT 14")
        rows = cur.fetchall()
        if len(rows) >= 2:
            rows.reverse()
            dvol = float(rows[-1]['close'])
            recent_cross = False
            for thr in [60, 80]:
                for i in range(1, min(8, len(rows))):
                    p, c = float(rows[i-1]['close']), float(rows[i]['close'])
                    if (p < thr and c >= thr) or (p > thr and c <= thr): recent_cross = True; break
            rules_iv = [
                {"name": "DVOL threshold crossing", "active": recent_cross,
                 "detail": f"DVOL at {dvol:.1f} — crossed key level" if recent_cross else f"DVOL at {dvol:.1f}",
                 "context": "DVOL crossing 60 or 80 = regime shift in implied vol."},
                {"name": "DVOL extreme", "active": dvol > 90, "detail": f"DVOL at {dvol:.1f}",
                 "context": "DVOL above 90 = market expects a major move."},
            ]
            if rv30:
                spread = dvol - rv30
                rules_iv.append({"name": "IV-RV spread extreme", "active": abs(spread) > 25,
                    "detail": f"DVOL {dvol:.1f} vs RV {rv30:.1f} (spread {spread:+.1f})",
                    "context": "Large spread = market mispricing vol direction."})
    except: pass

    result = []
    if rules_rv:
        result.append({"category": "Bitcoin", "chart_name": "Realised Volatility", "chart_tab": "bitcoin", "chart_key": "btc-realvol", "rules": rules_rv})
    if rules_iv:
        result.append({"category": "Bitcoin", "chart_name": "RV vs IV (DVOL)", "chart_tab": "bitcoin", "chart_key": "btc-rv-iv", "rules": rules_iv})
    return result


def _rules_funding(cur):
    try:
        cur.execute("SELECT AVG(funding_rate) as avg_rate FROM funding_8h WHERE symbol = 'BTC' GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 14")
        rows = cur.fetchall()
    except: return []
    if len(rows) < 2: return []
    rows.reverse()
    current = float(rows[-1]['avg_rate']) if rows[-1]['avg_rate'] else 0
    ann = current * 3 * 365 * 100
    sign_flip = False
    for i in range(1, min(8, len(rows))):
        r0 = float(rows[i-1]['avg_rate']) if rows[i-1]['avg_rate'] else 0
        r1 = float(rows[i]['avg_rate']) if rows[i]['avg_rate'] else 0
        if (r0 < 0 and r1 >= 0) or (r0 > 0 and r1 <= 0): sign_flip = True; break
    rules = [
        {"name": "Sign flip", "active": sign_flip,
         "detail": f"Funding just flipped {'positive' if current >= 0 else 'negative'}" if sign_flip else f"Avg: {current*100:.4f}% ({ann:.0f}% ann.)",
         "context": "Funding flipping sign = positioning reversal."},
        {"name": "Extreme reading", "active": current > 0.0005 or current < -0.0003,
         "detail": f"Avg: {current*100:.4f}% ({ann:.0f}% ann.)",
         "context": "Extreme positive = crowded longs. Extreme negative = squeeze risk."},
    ]
    return [{"category": "Bitcoin", "chart_name": "Funding Rate", "chart_tab": "bitcoin", "chart_key": "btc-funding", "rules": rules}]


def _rules_dominance(cur):
    try:
        cur.execute("""
            SELECT b.market_cap_usd as btc_mcap, t.total_mcap_usd as total_mcap
            FROM marketcap_daily b JOIN total_marketcap_daily t ON b.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY b.timestamp::date DESC LIMIT 60
        """)
        rows = cur.fetchall()
    except: return []
    if len(rows) < 8: return []
    rows.reverse()
    doms = [float(r['btc_mcap']) / float(r['total_mcap']) * 100 for r in rows]
    delta = doms[-1] - (doms[-30] if len(doms) >= 30 else doms[0])
    rules = [{"name": "Major rotation", "active": abs(delta) > 3,
              "detail": f"{doms[-1]:.1f}% (30d change {delta:+.1f}pp)",
              "context": "Dominance shifting >3pp = significant capital rotation."}]
    return [{"category": "Bitcoin", "chart_name": "Market Dominance (%)", "chart_tab": "bitcoin", "chart_key": "btc-dominance", "rules": rules}]


def _rules_eth_btc(cur):
    try:
        cur.execute("""
            SELECT e.price_usd / b.price_usd as ratio FROM price_daily e
            JOIN price_daily b ON e.timestamp::date = b.timestamp::date
            WHERE e.symbol = 'ETH' AND b.symbol = 'BTC' AND e.price_usd > 0 AND b.price_usd > 0
            ORDER BY e.timestamp DESC LIMIT 60
        """)
        rows = cur.fetchall()
    except: return []
    if len(rows) < 8: return []
    rows.reverse()
    ratios = [float(r['ratio']) for r in rows]
    delta = ((ratios[-1] / (ratios[-30] if len(ratios) >= 30 else ratios[0])) - 1) * 100
    rules = [{"name": "Major shift", "active": abs(delta) > 10,
              "detail": f"{ratios[-1]:.5f} (30d change {delta:+.1f}%)",
              "context": "ETH/BTC moving >10% in a month = major risk appetite shift."}]
    return [{"category": "Ethereum", "chart_name": "ETH/BTC Ratio", "chart_tab": "ethereum", "chart_key": "eth-btc-ratio", "rules": rules}]


def _rules_alt_share(cur):
    try:
        cur.execute("""
            SELECT t.total_mcap_usd as total, b.market_cap_usd as btc, e.market_cap_usd as eth
            FROM total_marketcap_daily t
            JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
            JOIN marketcap_daily e ON e.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
              AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY t.timestamp::date DESC LIMIT 60
        """)
        rows = cur.fetchall()
    except: return []
    if len(rows) < 8: return []
    rows.reverse()
    shares = [round((float(r['total']) - float(r['btc']) - float(r['eth'])) / float(r['total']) * 100, 2) for r in rows]
    delta = shares[-1] - (shares[-30] if len(shares) >= 30 else shares[0])
    rules = [{"name": "Alt share shifting", "active": abs(delta) > 3,
              "detail": f"{shares[-1]:.1f}% (30d change {delta:+.1f}pp)",
              "context": "Alt share rising >3pp = capital rotating into risk assets."}]
    return [{"category": "Altcoins", "chart_name": "Dominance Shares", "chart_tab": "altcoins", "chart_key": "am-dominance", "rules": rules}]


def _fetch_alt_mcap(cur, limit=250):
    cur.execute("""
        SELECT t.total_mcap_usd - b.market_cap_usd - e.market_cap_usd as alt_mcap
        FROM total_marketcap_daily t
        JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
        JOIN marketcap_daily e ON e.timestamp::date = t.timestamp::date
        WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
          AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
        ORDER BY t.timestamp::date DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    if not rows: return []
    rows.reverse()
    return [float(r['alt_mcap']) for r in rows]


def handle_control_center(params):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    charts = []

    _, btc_prices = _fetch_prices(cur, 'BTC')
    charts.extend(_rules_ma_gap(btc_prices, "Bitcoin", "bitcoin", "BTC"))
    charts.extend(_rules_200w_deviation(btc_prices, "Bitcoin", "bitcoin", "BTC"))
    charts.extend(_rules_drawdown(btc_prices, "Bitcoin", "bitcoin", "BTC"))
    charts.extend(_rules_volatility(cur, btc_prices))
    charts.extend(_rules_funding(cur))
    charts.extend(_rules_dominance(cur))

    _, eth_prices = _fetch_prices(cur, 'ETH')
    charts.extend(_rules_ma_gap(eth_prices, "Ethereum", "ethereum", "ETH"))
    charts.extend(_rules_200w_deviation(eth_prices, "Ethereum", "ethereum", "ETH"))
    charts.extend(_rules_drawdown(eth_prices, "Ethereum", "ethereum", "ETH"))
    charts.extend(_rules_eth_btc(cur))

    alt_mcaps = _fetch_alt_mcap(cur)
    if alt_mcaps:
        charts.extend(_rules_ma_gap(alt_mcaps, "Altcoins", "altcoins", "Alt"))
    charts.extend(_rules_alt_share(cur))

    conn.close()
    return {"updated": datetime.now().strftime("%Y-%m-%d %H:%M UTC"), "charts": charts}
