"""
api/control_center.py — Signal matrix / control center
Computes status (green/yellow/red/grey) and trend (up/down/flat) for each indicator.
Each indicator specifies which chart it links to (tab + key).
Designed to be extended one indicator at a time.
"""
import math
from datetime import datetime, timedelta
from api.shared import get_conn
import psycopg2.extras


def _sma(prices, window):
    result = []
    for i in range(len(prices)):
        if i < window - 1:
            result.append(None)
        else:
            result.append(sum(prices[i - window + 1:i + 1]) / window)
    return result


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


# ══════════════════════════════════════════════════════════════════════════════
# INDIVIDUAL SIGNAL FUNCTIONS
# Each returns a dict with: id, name, chart_name, chart_tab, chart_key,
#                            group, status, trend, detail
# ══════════════════════════════════════════════════════════════════════════════

def _signal_ma_cross(dates, prices):
    """50d/200d MA golden/death cross proximity."""
    if len(prices) < 200:
        return None

    ma50  = _sma(prices, 50)
    ma200 = _sma(prices, 200)
    m50, m200 = ma50[-1], ma200[-1]
    if m50 is None or m200 is None or m200 == 0:
        return None

    gap_pct = (m50 / m200 - 1) * 100

    # Check cross in last 7 days
    recent_cross = False
    for i in range(-7, 0):
        idx = len(ma50) + i
        if idx < 1: continue
        if ma50[idx] and ma50[idx-1] and ma200[idx] and ma200[idx-1] and ma200[idx] > 0 and ma200[idx-1] > 0:
            was_below = ma50[idx-1] < ma200[idx-1]
            now_above = ma50[idx] >= ma200[idx]
            was_above = ma50[idx-1] > ma200[idx-1]
            now_below = ma50[idx] <= ma200[idx]
            if (was_below and now_above) or (was_above and now_below):
                recent_cross = True
                break

    if recent_cross or abs(gap_pct) < 3:
        status = "green"
    elif abs(gap_pct) < 10:
        status = "yellow"
    else:
        status = "grey"

    gap_prev = ((ma50[-8] / ma200[-8]) - 1) * 100 if ma50[-8] and ma200[-8] and ma200[-8] > 0 else None
    if gap_prev is not None:
        delta = abs(gap_pct) - abs(gap_prev)
        trend = "down" if delta > 0.3 else ("up" if delta < -0.3 else "flat")
    else:
        trend = "flat"

    position = "above" if gap_pct > 0 else "below"
    return {
        "id":         "ma-cross",
        "name":       "50d / 200d MA Cross",
        "chart_name": "Moving Averages",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-ma",
        "group":      "Moving Averages",
        "status":     status,
        "trend":      trend,
        "detail":     f"50d MA {abs(gap_pct):.1f}% {position} 200d MA" + (" · crossed recently" if recent_cross else ""),
    }


def _signal_price_vs_200d(dates, prices):
    """Price relative to 200d MA."""
    if len(prices) < 200:
        return None

    ma200 = _sma(prices, 200)
    m200, price = ma200[-1], prices[-1]
    if m200 is None or m200 == 0:
        return None

    dev_pct = (price / m200 - 1) * 100

    recent_cross = False
    for i in range(-7, 0):
        idx = len(prices) + i
        if idx < 1 or not ma200[idx] or not ma200[idx-1]: continue
        if (prices[idx-1] < ma200[idx-1] and prices[idx] >= ma200[idx]) or \
           (prices[idx-1] > ma200[idx-1] and prices[idx] <= ma200[idx]):
            recent_cross = True
            break

    if recent_cross or abs(dev_pct) < 3:
        status = "green"
    elif dev_pct < -20:
        status = "red"
    elif dev_pct < -5 or dev_pct > 50:
        status = "yellow"
    else:
        status = "grey"

    dev_prev = (prices[-8] / ma200[-8] - 1) * 100 if len(prices) > 8 and ma200[-8] and ma200[-8] > 0 else None
    trend = "up" if dev_prev and dev_pct > dev_prev + 1 else ("down" if dev_prev and dev_pct < dev_prev - 1 else "flat")

    position = "above" if dev_pct > 0 else "below"
    return {
        "id":         "price-200d",
        "name":       "Price vs 200d MA",
        "chart_name": "Moving Averages",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-ma",
        "group":      "Moving Averages",
        "status":     status,
        "trend":      trend,
        "detail":     f"BTC {abs(dev_pct):.1f}% {position} 200d MA",
    }


def _signal_200d_deviation(dates, prices):
    """200d MA % deviation — mean reversion signal."""
    if len(prices) < 200:
        return None

    ma200 = _sma(prices, 200)
    m200, price = ma200[-1], prices[-1]
    if m200 is None or m200 == 0:
        return None

    dev = (price / m200 - 1) * 100

    if abs(dev) < 5:
        status = "green"
    elif dev > 60 or dev < -30:
        status = "red"
    elif dev > 30 or dev < -15:
        status = "yellow"
    else:
        status = "grey"

    dev_prev = (prices[-8] / ma200[-8] - 1) * 100 if len(prices) > 8 and ma200[-8] and ma200[-8] > 0 else None
    trend = "up" if dev_prev and dev > dev_prev + 1 else ("down" if dev_prev and dev < dev_prev - 1 else "flat")

    return {
        "id":         "200d-deviation",
        "name":       "200d MA Deviation",
        "chart_name": "200d MA Deviation",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-200d-dev",
        "group":      "Moving Averages",
        "status":     status,
        "trend":      trend,
        "detail":     f"{dev:+.1f}% from 200d MA",
    }


def _signal_200w_floor(dates, prices):
    """Price relative to 200-week (1400d) MA — the macro floor."""
    if len(prices) < 1400:
        return None

    ma200w = _sma(prices, 1400)
    m200w, price = ma200w[-1], prices[-1]
    if m200w is None or m200w == 0:
        return None

    mult = price / m200w
    dev = (mult - 1) * 100

    if dev < 10:
        status = "red"     # very close to or below the floor
    elif dev < 30:
        status = "yellow"
    elif mult > 5:
        status = "yellow"  # extremely overextended
    else:
        status = "grey"

    dev_prev = (prices[-8] / ma200w[-8]) if len(prices) > 8 and ma200w[-8] and ma200w[-8] > 0 else None
    trend = "up" if dev_prev and mult > dev_prev + 0.05 else ("down" if dev_prev and mult < dev_prev - 0.05 else "flat")

    return {
        "id":         "200w-floor",
        "name":       "200-Week MA Floor",
        "chart_name": "200-Week MA Floor",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-200w-floor",
        "group":      "Moving Averages",
        "status":     status,
        "trend":      trend,
        "detail":     f"BTC at {mult:.2f}x the 200-week MA",
    }


def _signal_drawdown(dates, prices):
    """Drawdown from ATH."""
    if not prices:
        return None

    running_max = max(prices)
    dd = (prices[-1] / running_max - 1) * 100

    if dd > -5:
        status = "green"
    elif dd > -20:
        status = "yellow"
    else:
        status = "red"

    # 7d ago drawdown
    rm_7d = max(prices[:-7]) if len(prices) > 7 else running_max
    dd_7d = (prices[-8] / rm_7d - 1) * 100 if len(prices) > 8 else dd
    trend = "up" if dd > dd_7d + 1 else ("down" if dd < dd_7d - 1 else "flat")

    return {
        "id":         "drawdown-ath",
        "name":       "Drawdown from ATH",
        "chart_name": "Drawdown from ATH",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-drawdown",
        "group":      "Risk",
        "status":     status,
        "trend":      trend,
        "detail":     f"{dd:.1f}% from all-time high",
    }


def _signal_realvol(dates, prices):
    """30d realized volatility regime."""
    if len(prices) < 35:
        return None

    log_rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
    if len(log_rets) < 30:
        return None

    rets_30 = log_rets[-30:]
    mean = sum(rets_30) / len(rets_30)
    std = math.sqrt(sum((r - mean)**2 for r in rets_30) / len(rets_30))
    vol_30 = std * math.sqrt(365) * 100

    rets_prev = log_rets[-37:-7]
    if len(rets_prev) >= 30:
        mean_p = sum(rets_prev) / len(rets_prev)
        std_p = math.sqrt(sum((r - mean_p)**2 for r in rets_prev) / len(rets_prev))
        vol_prev = std_p * math.sqrt(365) * 100
    else:
        vol_prev = vol_30

    if vol_30 > 90:
        status = "red"
    elif vol_30 > 60:
        status = "yellow"
    elif vol_30 < 30:
        status = "green"  # unusually low — something may be brewing
    else:
        status = "grey"

    trend = "up" if vol_30 > vol_prev + 3 else ("down" if vol_30 < vol_prev - 3 else "flat")

    return {
        "id":         "realvol-30d",
        "name":       "30d Realized Volatility",
        "chart_name": "Realized Volatility",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-realvol",
        "group":      "Volatility",
        "status":     status,
        "trend":      trend,
        "detail":     f"{vol_30:.1f}% annualized",
    }


def _signal_dvol(cur):
    """DVOL implied volatility regime."""
    cur.execute("""
        SELECT timestamp::date as date, close
        FROM dvol_daily
        WHERE currency = 'BTC'
        ORDER BY timestamp DESC
        LIMIT 14
    """)
    rows = cur.fetchall()
    if len(rows) < 2:
        return None

    rows.reverse()
    current = float(rows[-1]['close'])
    prev_7d = float(rows[-8]['close']) if len(rows) >= 8 else float(rows[0]['close'])

    recent_cross = False
    for threshold in [60, 80]:
        for i in range(1, min(8, len(rows))):
            prev = float(rows[i-1]['close'])
            curr = float(rows[i]['close'])
            if (prev < threshold and curr >= threshold) or (prev > threshold and curr <= threshold):
                recent_cross = True
                break

    if recent_cross:
        status = "green"
    elif current > 90:
        status = "red"
    elif current > 70:
        status = "yellow"
    else:
        status = "grey"

    trend = "up" if current > prev_7d + 2 else ("down" if current < prev_7d - 2 else "flat")

    return {
        "id":         "dvol",
        "name":       "DVOL (Implied Vol)",
        "chart_name": "—",
        "chart_tab":  None,
        "chart_key":  None,
        "group":      "Volatility",
        "status":     status,
        "trend":      trend,
        "detail":     f"DVOL: {current:.1f}",
    }


def _signal_funding(cur):
    """BTC perpetual funding rate regime."""
    cur.execute("""
        SELECT timestamp::date as date, AVG(funding_rate) as avg_rate
        FROM funding_8h
        WHERE symbol = 'BTC'
        GROUP BY timestamp::date
        ORDER BY timestamp::date DESC
        LIMIT 14
    """)
    rows = cur.fetchall()
    if len(rows) < 2:
        return None

    rows.reverse()
    current = float(rows[-1]['avg_rate']) if rows[-1]['avg_rate'] else 0
    prev_7d = float(rows[-8]['avg_rate']) if len(rows) >= 8 and rows[-8]['avg_rate'] else 0

    sign_flip = False
    for i in range(1, min(8, len(rows))):
        r0 = float(rows[i-1]['avg_rate']) if rows[i-1]['avg_rate'] else 0
        r1 = float(rows[i]['avg_rate']) if rows[i]['avg_rate'] else 0
        if (r0 < 0 and r1 >= 0) or (r0 > 0 and r1 <= 0):
            sign_flip = True
            break

    ann = current * 3 * 365 * 100

    if sign_flip:
        status = "green"
    elif current > 0.0005 or current < -0.0003:
        status = "red"
    elif current > 0.0003 or current < -0.0001:
        status = "yellow"
    else:
        status = "grey"

    trend = "up" if current > prev_7d + 0.0001 else ("down" if current < prev_7d - 0.0001 else "flat")

    return {
        "id":         "funding",
        "name":       "Funding Rate",
        "chart_name": "Funding Rate",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-funding",
        "group":      "Derivatives",
        "status":     status,
        "trend":      trend,
        "detail":     f"Avg daily: {current*100:.4f}% ({ann:.1f}% ann.)",
    }


def _signal_dominance(cur):
    """BTC market dominance trend."""
    try:
        cur.execute("""
            SELECT b.timestamp::date as date,
                   b.market_cap_usd as btc_mcap,
                   t.total_mcap_usd as total_mcap
            FROM marketcap_daily b
            JOIN total_marketcap_daily t ON b.timestamp::date = t.timestamp::date
            WHERE b.coingecko_id = (SELECT coingecko_id FROM asset_registry WHERE symbol = 'BTC' LIMIT 1)
              AND b.market_cap_usd > 0 AND t.total_mcap_usd > 0
            ORDER BY b.timestamp::date DESC
            LIMIT 60
        """)
        rows = cur.fetchall()
    except:
        return None

    if len(rows) < 8:
        return None

    rows.reverse()
    doms = [float(r['btc_mcap']) / float(r['total_mcap']) * 100 for r in rows]
    current = doms[-1]
    prev_7d = doms[-8] if len(doms) >= 8 else doms[0]
    prev_30d = doms[-30] if len(doms) >= 30 else doms[0]

    # Dominance rising = capital flowing into BTC (risk-off for alts)
    delta_30d = current - prev_30d

    if abs(delta_30d) > 5:
        status = "green"  # big shift happening
    elif abs(delta_30d) > 2:
        status = "yellow"
    else:
        status = "grey"

    trend = "up" if current > prev_7d + 0.3 else ("down" if current < prev_7d - 0.3 else "flat")

    return {
        "id":         "btc-dominance",
        "name":       "BTC Dominance",
        "chart_name": "BTC Market Dominance",
        "chart_tab":  "bitcoin",
        "chart_key":  "btc-dominance",
        "group":      "Market Structure",
        "status":     status,
        "trend":      trend,
        "detail":     f"{current:.1f}% (30d Δ {delta_30d:+.1f}pp)",
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ══════════════════════════════════════════════════════════════════════════════

def handle_control_center(params):
    """Compute all signals and return the matrix."""
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    dates, prices = _fetch_btc_prices(cur)

    signals = []

    # Price-based signals
    for fn in [_signal_ma_cross, _signal_price_vs_200d, _signal_200d_deviation,
               _signal_200w_floor, _signal_drawdown, _signal_realvol]:
        try:
            s = fn(dates, prices)
            if s: signals.append(s)
        except Exception as e:
            pass

    # DB-based signals
    for fn in [_signal_dvol, _signal_funding, _signal_dominance]:
        try:
            s = fn(cur)
            if s: signals.append(s)
        except Exception as e:
            pass

    conn.close()

    return {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
        "signals": signals,
    }
