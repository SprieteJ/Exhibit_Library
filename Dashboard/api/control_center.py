"""
api/control_center.py — Signal matrix / control center
Each signal: id, name, chart_name, chart_tab, chart_key, group, status, trend, detail, context
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn
import psycopg2.extras


def _sma(prices, window):
    return [None if i < window - 1 else sum(prices[i - window + 1:i + 1]) / window
            for i in range(len(prices))]


def _fetch_btc_prices(cur, days_back=1500):
    dt_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT timestamp::date as date, price_usd
        FROM price_daily
        WHERE symbol = 'BTC' AND timestamp >= %s AND price_usd > 0
        ORDER BY timestamp
    """, (dt_from,))
    rows = cur.fetchall()
    return [str(r['date']) for r in rows], [float(r['price_usd']) for r in rows]


def _signal_ma_cross(dates, prices):
    if len(prices) < 200: return None
    ma50, ma200 = _sma(prices, 50), _sma(prices, 200)
    m50, m200 = ma50[-1], ma200[-1]
    if not m50 or not m200 or m200 == 0: return None
    gap = (m50 / m200 - 1) * 100

    recent_cross = False
    for i in range(-7, 0):
        idx = len(ma50) + i
        if idx < 1 or not ma50[idx] or not ma50[idx-1] or not ma200[idx] or not ma200[idx-1]: continue
        if (ma50[idx-1] < ma200[idx-1] and ma50[idx] >= ma200[idx]) or \
           (ma50[idx-1] > ma200[idx-1] and ma50[idx] <= ma200[idx]):
            recent_cross = True; break

    status = "green" if (recent_cross or abs(gap) < 3) else ("yellow" if abs(gap) < 10 else "grey")
    gap_prev = ((ma50[-8] / ma200[-8]) - 1) * 100 if ma50[-8] and ma200[-8] and ma200[-8] > 0 else None
    trend = "flat"
    if gap_prev is not None:
        d = abs(gap) - abs(gap_prev)
        trend = "down" if d > 0.3 else ("up" if d < -0.3 else "flat")
    pos = "above" if gap > 0 else "below"

    return {"id": "ma-cross", "name": "50d / 200d MA Cross",
            "chart_name": "MA Gap", "chart_tab": "bitcoin", "chart_key": "btc-ma-gap",
            "group": "Moving Averages", "status": status, "trend": trend,
            "detail": f"50d MA {abs(gap):.1f}% {pos} 200d MA" + (" · crossed recently" if recent_cross else ""),
            "context": "Golden cross = bullish trend confirmation. Death cross = sustained weakness. The gap velocity matters more than the level."}


def _signal_price_vs_200d(dates, prices):
    if len(prices) < 200: return None
    ma200 = _sma(prices, 200)
    m200, price = ma200[-1], prices[-1]
    if not m200 or m200 == 0: return None
    dev = (price / m200 - 1) * 100

    recent_cross = False
    for i in range(-7, 0):
        idx = len(prices) + i
        if idx < 1 or not ma200[idx] or not ma200[idx-1]: continue
        if (prices[idx-1] < ma200[idx-1] and prices[idx] >= ma200[idx]) or \
           (prices[idx-1] > ma200[idx-1] and prices[idx] <= ma200[idx]):
            recent_cross = True; break

    if recent_cross or abs(dev) < 3: status = "green"
    elif dev < -20: status = "red"
    elif dev < -5 or dev > 50: status = "yellow"
    else: status = "grey"

    dev_prev = (prices[-8] / ma200[-8] - 1) * 100 if len(prices) > 8 and ma200[-8] and ma200[-8] > 0 else None
    trend = "up" if dev_prev and dev > dev_prev + 1 else ("down" if dev_prev and dev < dev_prev - 1 else "flat")
    pos = "above" if dev > 0 else "below"

    return {"id": "price-200d", "name": "Price vs 200d MA",
            "chart_name": "Moving Averages", "chart_tab": "bitcoin", "chart_key": "btc-ma",
            "group": "Moving Averages", "status": status, "trend": trend,
            "detail": f"BTC {abs(dev):.1f}% {pos} 200d MA",
            "context": "Price crossing the 200d MA is the most widely followed trend signal. Sustained breaks above/below shift the macro regime."}


def _signal_200d_deviation(dates, prices):
    if len(prices) < 200: return None
    ma200 = _sma(prices, 200)
    m200, price = ma200[-1], prices[-1]
    if not m200 or m200 == 0: return None
    dev = (price / m200 - 1) * 100

    if abs(dev) < 5: status = "green"
    elif dev > 60 or dev < -30: status = "red"
    elif dev > 30 or dev < -15: status = "yellow"
    else: status = "grey"

    dev_prev = (prices[-8] / ma200[-8] - 1) * 100 if len(prices) > 8 and ma200[-8] and ma200[-8] > 0 else None
    trend = "up" if dev_prev and dev > dev_prev + 1 else ("down" if dev_prev and dev < dev_prev - 1 else "flat")

    return {"id": "200d-deviation", "name": "200d MA Deviation",
            "chart_name": "200d MA Deviation", "chart_tab": "bitcoin", "chart_key": "btc-200d-dev",
            "group": "Moving Averages", "status": status, "trend": trend,
            "detail": f"{dev:+.1f}% from 200d MA",
            "context": ">50% historically marks blow-off tops. <-30% marks capitulation lows. Mean-reversion is the dominant regime."}


def _signal_200w_floor(dates, prices):
    if len(prices) < 1400: return None
    ma200w = _sma(prices, 1400)
    m200w, price = ma200w[-1], prices[-1]
    if not m200w or m200w == 0: return None
    mult = price / m200w

    if mult < 1.1: status = "red"
    elif mult < 1.3: status = "yellow"
    elif mult > 5: status = "yellow"
    else: status = "grey"

    prev = (prices[-8] / ma200w[-8]) if len(prices) > 8 and ma200w[-8] and ma200w[-8] > 0 else None
    trend = "up" if prev and mult > prev + 0.05 else ("down" if prev and mult < prev - 0.05 else "flat")

    return {"id": "200w-floor", "name": "200-Week MA Floor",
            "chart_name": "200-Week MA Floor", "chart_tab": "bitcoin", "chart_key": "btc-200w-floor",
            "group": "Moving Averages", "status": status, "trend": trend,
            "detail": f"BTC at {mult:.2f}x the 200-week MA",
            "context": "BTC has never sustained a close below the 200-week MA. Touching it = generational buy signal. >5x = overheated."}


def _signal_pi_cycle(dates, prices):
    if len(prices) < 350: return None
    ma111 = _sma(prices, 111)
    ma350 = _sma(prices, 350)
    m111, m350 = ma111[-1], ma350[-1]
    if not m111 or not m350 or m350 == 0: return None
    m350x2 = m350 * 2
    gap = (m111 / m350x2 - 1) * 100

    recent_cross = False
    for i in range(-7, 0):
        idx = len(ma111) + i
        if idx < 1 or not ma111[idx] or not ma111[idx-1] or not ma350[idx] or not ma350[idx-1]: continue
        t2_now = ma350[idx] * 2; t2_prev = ma350[idx-1] * 2
        if ma111[idx-1] < t2_prev and ma111[idx] >= t2_now:
            recent_cross = True; break

    if recent_cross or abs(gap) < 2: status = "green"
    elif gap > -5: status = "yellow"
    else: status = "grey"

    gap_prev = ((ma111[-8] / (ma350[-8] * 2)) - 1) * 100 if ma111[-8] and ma350[-8] and ma350[-8] > 0 else None
    trend = "up" if gap_prev and gap > gap_prev + 0.5 else ("down" if gap_prev and gap < gap_prev - 0.5 else "flat")

    return {"id": "pi-cycle", "name": "Pi Cycle Top",
            "chart_name": "Pi Cycle Top", "chart_tab": "bitcoin", "chart_key": "btc-pi-cycle",
            "group": "Cycle Indicators", "status": status, "trend": trend,
            "detail": f"111d MA {abs(gap):.1f}% {'above' if gap > 0 else 'below'} 2×350d MA",
            "context": "When the 111d MA crosses above 2×350d MA, it has called every BTC cycle top within 3 days. Data back to 2013."}


def _signal_drawdown(dates, prices):
    if not prices: return None
    running_max = max(prices)
    dd = (prices[-1] / running_max - 1) * 100

    if dd > -5: status = "green"
    elif dd > -20: status = "yellow"
    else: status = "red"

    rm_7d = max(prices[:-7]) if len(prices) > 7 else running_max
    dd_7d = (prices[-8] / rm_7d - 1) * 100 if len(prices) > 8 else dd
    trend = "up" if dd > dd_7d + 1 else ("down" if dd < dd_7d - 1 else "flat")

    return {"id": "drawdown-ath", "name": "Drawdown from ATH",
            "chart_name": "Drawdown from ATH", "chart_tab": "bitcoin", "chart_key": "btc-drawdown",
            "group": "Risk", "status": status, "trend": trend,
            "detail": f"{dd:.1f}% from all-time high",
            "context": "Near ATH = nothing to watch. >20% historically marks bear territory. >50% = deep capitulation zone."}


def _signal_realvol(dates, prices):
    if len(prices) < 35: return None
    log_rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
    if len(log_rets) < 30: return None

    rets_30 = log_rets[-30:]
    mean = sum(rets_30) / len(rets_30)
    std = math.sqrt(sum((r - mean)**2 for r in rets_30) / len(rets_30))
    vol_30 = std * math.sqrt(365) * 100

    rets_prev = log_rets[-37:-7]
    vol_prev = vol_30
    if len(rets_prev) >= 30:
        mp = sum(rets_prev) / len(rets_prev)
        sp = math.sqrt(sum((r - mp)**2 for r in rets_prev) / len(rets_prev))
        vol_prev = sp * math.sqrt(365) * 100

    if vol_30 > 90: status = "red"
    elif vol_30 > 60: status = "yellow"
    elif vol_30 < 30: status = "green"
    else: status = "grey"

    trend = "up" if vol_30 > vol_prev + 3 else ("down" if vol_30 < vol_prev - 3 else "flat")

    return {"id": "realvol-30d", "name": "30d Realized Volatility",
            "chart_name": "Realized Volatility", "chart_tab": "bitcoin", "chart_key": "btc-realvol",
            "group": "Volatility", "status": status, "trend": trend,
            "detail": f"{vol_30:.1f}% annualized",
            "context": "Unusually low vol (<30%) often precedes explosive moves. >80% = crisis-level volatility, expect mean reversion."}


def _signal_dvol(cur):
    cur.execute("SELECT close FROM dvol_daily WHERE currency = 'BTC' ORDER BY timestamp DESC LIMIT 14")
    rows = cur.fetchall()
    if len(rows) < 2: return None
    rows.reverse()
    current = float(rows[-1]['close'])
    prev_7d = float(rows[-8]['close']) if len(rows) >= 8 else float(rows[0]['close'])

    recent_cross = False
    for threshold in [60, 80]:
        for i in range(1, min(8, len(rows))):
            p, c = float(rows[i-1]['close']), float(rows[i]['close'])
            if (p < threshold and c >= threshold) or (p > threshold and c <= threshold):
                recent_cross = True; break

    if recent_cross: status = "green"
    elif current > 90: status = "red"
    elif current > 70: status = "yellow"
    else: status = "grey"

    trend = "up" if current > prev_7d + 2 else ("down" if current < prev_7d - 2 else "flat")

    return {"id": "dvol", "name": "DVOL (Implied Vol)",
            "chart_name": "—", "chart_tab": None, "chart_key": None,
            "group": "Volatility", "status": status, "trend": trend,
            "detail": f"DVOL: {current:.1f}",
            "context": "30-day forward implied vol from options. >80 = market pricing a major move. Crossing 60 up/down marks regime shifts."}


def _signal_rv_iv(cur):
    cur.execute("SELECT close FROM dvol_daily WHERE currency = 'BTC' ORDER BY timestamp DESC LIMIT 1")
    dvol_row = cur.fetchone()
    if not dvol_row: return None
    dvol_now = float(dvol_row['close'])

    cur.execute("SELECT price_usd FROM price_daily WHERE symbol = 'BTC' AND price_usd > 0 ORDER BY timestamp DESC LIMIT 35")
    price_rows = cur.fetchall()
    if len(price_rows) < 31: return None
    price_rows.reverse()
    prices = [float(r['price_usd']) for r in price_rows]
    log_rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
    rets = log_rets[-30:]
    mean = sum(rets) / len(rets)
    std = math.sqrt(sum((r - mean)**2 for r in rets) / len(rets))
    rv30 = std * math.sqrt(365) * 100
    spread = dvol_now - rv30

    if abs(spread) > 30: status = "red"
    elif abs(spread) > 20: status = "yellow"
    else: status = "grey"

    trend = "up" if spread > 5 else ("down" if spread < -5 else "flat")

    return {"id": "rv-iv", "name": "IV-RV Spread",
            "chart_name": "RV vs IV", "chart_tab": "bitcoin", "chart_key": "btc-rv-iv",
            "group": "Volatility", "status": status, "trend": trend,
            "detail": f"DVOL {dvol_now:.1f} vs RV30 {rv30:.1f} (spread: {spread:+.1f})",
            "context": "Positive spread = market expects more vol than realized. Negative = complacency or vol already being realized."}


def _signal_funding(cur):
    cur.execute("""
        SELECT AVG(funding_rate) as avg_rate FROM funding_8h
        WHERE symbol = 'BTC' GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 14
    """)
    rows = cur.fetchall()
    if len(rows) < 2: return None
    rows.reverse()
    current = float(rows[-1]['avg_rate']) if rows[-1]['avg_rate'] else 0
    prev_7d = float(rows[-8]['avg_rate']) if len(rows) >= 8 and rows[-8]['avg_rate'] else 0

    sign_flip = False
    for i in range(1, min(8, len(rows))):
        r0 = float(rows[i-1]['avg_rate']) if rows[i-1]['avg_rate'] else 0
        r1 = float(rows[i]['avg_rate']) if rows[i]['avg_rate'] else 0
        if (r0 < 0 and r1 >= 0) or (r0 > 0 and r1 <= 0): sign_flip = True; break

    ann = current * 3 * 365 * 100
    if sign_flip: status = "green"
    elif current > 0.0005 or current < -0.0003: status = "red"
    elif current > 0.0003 or current < -0.0001: status = "yellow"
    else: status = "grey"

    trend = "up" if current > prev_7d + 0.0001 else ("down" if current < prev_7d - 0.0001 else "flat")

    return {"id": "funding", "name": "Funding Rate",
            "chart_name": "Funding Rate", "chart_tab": "bitcoin", "chart_key": "btc-funding",
            "group": "Derivatives", "status": status, "trend": trend,
            "detail": f"Avg daily: {current*100:.4f}% ({ann:.1f}% ann.)",
            "context": "Sign flip = positioning reversal. Extreme positive = crowded longs about to get flushed. Negative = shorts paying longs."}


def _signal_dominance(cur):
    try:
        cur.execute("""
            SELECT b.market_cap_usd as btc_mcap, t.total_mcap_usd as total_mcap
            FROM marketcap_daily b
            JOIN total_marketcap_daily t ON b.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY b.timestamp::date DESC LIMIT 60
        """)
        rows = cur.fetchall()
    except: return None
    if len(rows) < 8: return None

    rows.reverse()
    doms = [float(r['btc_mcap']) / float(r['total_mcap']) * 100 for r in rows]
    current, prev_7d = doms[-1], doms[-8] if len(doms) >= 8 else doms[0]
    prev_30d = doms[-30] if len(doms) >= 30 else doms[0]
    delta_30d = current - prev_30d

    if abs(delta_30d) > 5: status = "green"
    elif abs(delta_30d) > 2: status = "yellow"
    else: status = "grey"

    trend = "up" if current > prev_7d + 0.3 else ("down" if current < prev_7d - 0.3 else "flat")

    return {"id": "btc-dominance", "name": "BTC Dominance",
            "chart_name": "BTC Market Dominance", "chart_tab": "bitcoin", "chart_key": "btc-dominance",
            "group": "Market Structure", "status": status, "trend": trend,
            "detail": f"{current:.1f}% (30d Δ {delta_30d:+.1f}pp)",
            "context": "Rising dominance = risk-off rotation into BTC. Falling = capital flowing to alts (altseason conditions)."}


def _signal_btc_mcap(cur):
    cur.execute("""
        SELECT market_cap_usd FROM marketcap_daily
        WHERE coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
          AND market_cap_usd > 0 ORDER BY timestamp DESC LIMIT 1
    """)
    row = cur.fetchone()
    if not row: return None
    mcap = float(row['market_cap_usd'])

    milestones = [(2e12, "$2T"), (1e12, "$1T"), (5e11, "$500B")]
    nearest = None
    for val, label in milestones:
        pct = (mcap / val - 1) * 100
        if abs(pct) < 10: nearest = (label, pct); break

    if nearest and abs(nearest[1]) < 3: status = "green"
    elif nearest: status = "yellow"
    else: status = "grey"

    fmt = f"${mcap/1e12:.2f}T" if mcap >= 1e12 else f"${mcap/1e9:.0f}B"

    return {"id": "btc-mcap", "name": "BTC Market Cap",
            "chart_name": "BTC Market Cap", "chart_tab": "bitcoin", "chart_key": "btc-mcap",
            "group": "Market Structure", "status": status, "trend": "flat",
            "detail": fmt + (f" · {abs(nearest[1]):.1f}% {'above' if nearest[1] > 0 else 'below'} {nearest[0]}" if nearest else ""),
            "context": "Round-number milestones ($1T, $2T) act as psychological levels. Breaks above attract institutional attention and flows."}


def handle_control_center(params):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dates, prices = _fetch_btc_prices(cur)

    signals = []

    # BTC price-based signals
    for fn in [_signal_ma_cross, _signal_price_vs_200d, _signal_200d_deviation,
               _signal_200w_floor, _signal_pi_cycle, _signal_drawdown, _signal_realvol]:
        try:
            s = fn(dates, prices)
            if s: signals.append(s)
        except: pass

    # BTC DB-based signals
    for fn in [_signal_dvol, _signal_rv_iv, _signal_funding, _signal_dominance, _signal_btc_mcap]:
        try:
            s = fn(cur)
            if s: signals.append(s)
        except: pass

    # ETH signals
    try:
        eth_dates, eth_prices = _fetch_eth_prices(cur)
        for fn in [_signal_eth_ma_cross, _signal_eth_200d_dev, _signal_eth_drawdown]:
            try:
                s = fn(eth_dates, eth_prices)
                if s: signals.append(s)
            except: pass
        try:
            s = _signal_eth_btc_ratio(cur)
            if s: signals.append(s)
        except: pass
    except: pass

    # ALT signals
    try:
        for fn in [_signal_alt_mcap_cross, _signal_alt_mcap_dev, _signal_alt_share]:
            try:
                s = fn(cur)
                if s: signals.append(s)
            except: pass
    except: pass

    conn.close()
    return {"updated": datetime.now().strftime("%Y-%m-%d %H:%M UTC"), "signals": signals}


# ══════════════════════════════════════════════════════════════════════════════
# ETH SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_eth_prices(cur, days_back=1500):
    dt_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT timestamp::date as date, price_usd FROM price_daily
        WHERE symbol = 'ETH' AND timestamp >= %s AND price_usd > 0 ORDER BY timestamp
    """, (dt_from,))
    rows = cur.fetchall()
    return [str(r['date']) for r in rows], [float(r['price_usd']) for r in rows]


def _signal_eth_ma_cross(dates, prices):
    if len(prices) < 200: return None
    ma50, ma200 = _sma(prices, 50), _sma(prices, 200)
    m50, m200 = ma50[-1], ma200[-1]
    if not m50 or not m200 or m200 == 0: return None
    gap = (m50 / m200 - 1) * 100

    recent_cross = False
    for i in range(-7, 0):
        idx = len(ma50) + i
        if idx < 1 or not ma50[idx] or not ma50[idx-1] or not ma200[idx] or not ma200[idx-1]: continue
        if (ma50[idx-1] < ma200[idx-1] and ma50[idx] >= ma200[idx]) or \
           (ma50[idx-1] > ma200[idx-1] and ma50[idx] <= ma200[idx]):
            recent_cross = True; break

    status = "green" if (recent_cross or abs(gap) < 3) else ("yellow" if abs(gap) < 10 else "grey")
    gap_prev = ((ma50[-8] / ma200[-8]) - 1) * 100 if ma50[-8] and ma200[-8] and ma200[-8] > 0 else None
    trend = "flat"
    if gap_prev is not None:
        d = abs(gap) - abs(gap_prev)
        trend = "down" if d > 0.3 else ("up" if d < -0.3 else "flat")

    return {"id": "eth-ma-cross", "name": "ETH 50d/200d Cross",
            "chart_name": "ETH MA Gap", "chart_tab": "ethereum", "chart_key": "eth-ma-gap",
            "group": "Ethereum", "status": status, "trend": trend,
            "detail": f"50d MA {abs(gap):.1f}% {'above' if gap > 0 else 'below'} 200d MA",
            "context": "Same logic as BTC cross but ETH often leads or lags BTC by days/weeks. Divergence between the two is informative."}


def _signal_eth_200d_dev(dates, prices):
    if len(prices) < 200: return None
    ma200 = _sma(prices, 200)
    m200, price = ma200[-1], prices[-1]
    if not m200 or m200 == 0: return None
    dev = (price / m200 - 1) * 100

    if abs(dev) < 5: status = "green"
    elif dev > 60 or dev < -30: status = "red"
    elif dev > 30 or dev < -15: status = "yellow"
    else: status = "grey"

    dev_prev = (prices[-8] / ma200[-8] - 1) * 100 if len(prices) > 8 and ma200[-8] and ma200[-8] > 0 else None
    trend = "up" if dev_prev and dev > dev_prev + 1 else ("down" if dev_prev and dev < dev_prev - 1 else "flat")

    return {"id": "eth-200d-dev", "name": "ETH Deviation 200d MA",
            "chart_name": "ETH 200d Deviation", "chart_tab": "ethereum", "chart_key": "eth-200d-dev",
            "group": "Ethereum", "status": status, "trend": trend,
            "detail": f"{dev:+.1f}% from 200d MA",
            "context": "ETH often overshoots BTC in both directions. Extreme deviation + divergence from BTC = potential mean reversion trade."}


def _signal_eth_drawdown(dates, prices):
    if not prices: return None
    rm = max(prices)
    dd = (prices[-1] / rm - 1) * 100

    if dd > -5: status = "green"
    elif dd > -20: status = "yellow"
    else: status = "red"

    rm_7d = max(prices[:-7]) if len(prices) > 7 else rm
    dd_7d = (prices[-8] / rm_7d - 1) * 100 if len(prices) > 8 else dd
    trend = "up" if dd > dd_7d + 1 else ("down" if dd < dd_7d - 1 else "flat")

    return {"id": "eth-drawdown", "name": "ETH Drawdown from ATH",
            "chart_name": "ETH Drawdown", "chart_tab": "ethereum", "chart_key": "eth-drawdown",
            "group": "Ethereum", "status": status, "trend": trend,
            "detail": f"{dd:.1f}% from ATH",
            "context": "ETH drawdowns are typically deeper than BTC. >30% while BTC <15% signals relative ETH weakness."}


def _signal_eth_btc_ratio(cur):
    cur.execute("""
        SELECT e.price_usd / b.price_usd as ratio
        FROM price_daily e
        JOIN price_daily b ON e.timestamp::date = b.timestamp::date
        WHERE e.symbol = 'ETH' AND b.symbol = 'BTC' AND e.price_usd > 0 AND b.price_usd > 0
        ORDER BY e.timestamp DESC LIMIT 60
    """)
    rows = cur.fetchall()
    if len(rows) < 8: return None
    rows.reverse()
    ratios = [float(r['ratio']) for r in rows]
    current, prev_7d = ratios[-1], ratios[-8]
    prev_30d = ratios[-30] if len(ratios) >= 30 else ratios[0]
    delta_30d = ((current / prev_30d) - 1) * 100 if prev_30d > 0 else 0

    if abs(delta_30d) > 15: status = "green"
    elif abs(delta_30d) > 7: status = "yellow"
    else: status = "grey"

    trend = "up" if current > prev_7d * 1.01 else ("down" if current < prev_7d * 0.99 else "flat")

    return {"id": "eth-btc-ratio", "name": "ETH/BTC Ratio",
            "chart_name": "ETH/BTC Ratio", "chart_tab": "ethereum", "chart_key": "eth-btc-ratio",
            "group": "Ethereum", "status": status, "trend": trend,
            "detail": f"{current:.5f} (30d Δ {delta_30d:+.1f}%)",
            "context": "The ETH/BTC ratio is the single best gauge of altcoin risk appetite. Falling ratio = BTC dominance rising, risk-off."}


# ══════════════════════════════════════════════════════════════════════════════
# ALTCOIN SIGNALS
# ══════════════════════════════════════════════════════════════════════════════

def _signal_alt_mcap_cross(cur):
    try:
        cur.execute("""
            SELECT t.timestamp::date as date,
                   t.total_mcap_usd - b.market_cap_usd - e.market_cap_usd as alt_mcap
            FROM total_marketcap_daily t
            JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
            JOIN marketcap_daily e ON e.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
              AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY t.timestamp::date DESC LIMIT 210
        """)
        rows = cur.fetchall()
    except: return None
    if len(rows) < 200: return None
    rows.reverse()
    mcaps = [float(r['alt_mcap']) for r in rows]

    ma50  = _sma(mcaps, 50)
    ma200 = _sma(mcaps, 200)
    m50, m200 = ma50[-1], ma200[-1]
    if not m50 or not m200 or m200 == 0: return None
    gap = (m50 / m200 - 1) * 100

    recent_cross = False
    for i in range(-7, 0):
        idx = len(ma50) + i
        if idx < 1 or not ma50[idx] or not ma50[idx-1] or not ma200[idx] or not ma200[idx-1]: continue
        if (ma50[idx-1] < ma200[idx-1] and ma50[idx] >= ma200[idx]) or \
           (ma50[idx-1] > ma200[idx-1] and ma50[idx] <= ma200[idx]):
            recent_cross = True; break

    status = "green" if (recent_cross or abs(gap) < 3) else ("yellow" if abs(gap) < 10 else "grey")
    gap_prev = ((ma50[-8] / ma200[-8]) - 1) * 100 if ma50[-8] and ma200[-8] and ma200[-8] > 0 else None
    trend = "flat"
    if gap_prev:
        d = abs(gap) - abs(gap_prev)
        trend = "down" if d > 0.3 else ("up" if d < -0.3 else "flat")

    return {"id": "alt-mcap-cross", "name": "Altcoin Mcap 50d/200d Cross",
            "chart_name": "Altcoin Mcap", "chart_tab": "alt_market", "chart_key": "am-mcap",
            "group": "Altcoins", "status": status, "trend": trend,
            "detail": f"50d MA {abs(gap):.1f}% {'above' if gap > 0 else 'below'} 200d MA",
            "context": "Alt mcap golden cross = capital flowing into alts. Death cross = sustained outflow back to BTC or fiat."}


def _signal_alt_mcap_dev(cur):
    try:
        cur.execute("""
            SELECT t.total_mcap_usd - b.market_cap_usd - e.market_cap_usd as alt_mcap
            FROM total_marketcap_daily t
            JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
            JOIN marketcap_daily e ON e.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
              AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY t.timestamp::date DESC LIMIT 210
        """)
        rows = cur.fetchall()
    except: return None
    if len(rows) < 200: return None
    rows.reverse()
    mcaps = [float(r['alt_mcap']) for r in rows]

    ma200 = _sma(mcaps, 200)
    if not ma200[-1] or ma200[-1] == 0: return None
    dev = (mcaps[-1] / ma200[-1] - 1) * 100

    if abs(dev) < 5: status = "green"
    elif dev > 60 or dev < -30: status = "red"
    elif dev > 30 or dev < -15: status = "yellow"
    else: status = "grey"

    dev_prev = (mcaps[-8] / ma200[-8] - 1) * 100 if ma200[-8] and ma200[-8] > 0 else None
    trend = "up" if dev_prev and dev > dev_prev + 1 else ("down" if dev_prev and dev < dev_prev - 1 else "flat")

    return {"id": "alt-mcap-dev", "name": "Altcoin Deviation 200d MA",
            "chart_name": "Altcoin Deviation", "chart_tab": "alt_market", "chart_key": "am-mcap-dev",
            "group": "Altcoins", "status": status, "trend": trend,
            "detail": f"{dev:+.1f}% from 200d MA",
            "context": "Alt mcap deviation >50% = altseason euphoria. <-30% = max pain, capitulation territory. Mean reversion dominates."}


def _signal_alt_share(cur):
    try:
        cur.execute("""
            SELECT t.total_mcap_usd as total,
                   b.market_cap_usd as btc,
                   e.market_cap_usd as eth
            FROM total_marketcap_daily t
            JOIN marketcap_daily b ON b.timestamp::date = t.timestamp::date
            JOIN marketcap_daily e ON e.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND e.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'ETH' LIMIT 1)
              AND b.market_cap_usd > 0 AND e.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY t.timestamp::date DESC LIMIT 60
        """)
        rows = cur.fetchall()
    except: return None
    if len(rows) < 8: return None
    rows.reverse()
    shares = [round((float(r['total']) - float(r['btc']) - float(r['eth'])) / float(r['total']) * 100, 2) for r in rows]
    current, prev_7d = shares[-1], shares[-8]
    prev_30d = shares[-30] if len(shares) >= 30 else shares[0]
    delta_30d = current - prev_30d

    if abs(delta_30d) > 5: status = "green"
    elif abs(delta_30d) > 2: status = "yellow"
    else: status = "grey"

    trend = "up" if current > prev_7d + 0.3 else ("down" if current < prev_7d - 0.3 else "flat")

    return {"id": "alt-share", "name": "Altcoin Share of Total",
            "chart_name": "Dominance Chart", "chart_tab": "alt_market", "chart_key": "am-dominance",
            "group": "Altcoins", "status": status, "trend": trend,
            "detail": f"{current:.1f}% (30d Δ {delta_30d:+.1f}pp)",
            "context": "Rising alt share = capital rotating into risk. Combined with ETH/BTC ratio rising = full altseason. Falling = flight to BTC quality."}

