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

    price = prices[-1]
    gap = (m50 / m200 - 1) * 100
    gap_series = [(ma50[i] / ma200[i] - 1) * 100 if ma50[i] and ma200[i] and ma200[i] > 0 else None
                  for i in range(len(prices))]
    sl50 = _slope(ma50)
    sl_gap = _slope(gap_series)
    sl_gap_now = _last_valid(sl_gap)

    key = f"{prefix.lower()}-ma-gap" if prefix != "Alt" else "am-mcap-gap"
    rules = []

    # ── Rule 1: Cross just happened (event, major) ──
    # Check if gap changed sign within last 14 days
    cross_happened = False
    cross_days_ago = None
    cross_type = None
    valid_gaps = [(i, g) for i, g in enumerate(gap_series) if g is not None]
    if len(valid_gaps) >= 2:
        for j in range(len(valid_gaps) - 1, max(len(valid_gaps) - 15, 0), -1):
            idx_now, g_now = valid_gaps[j]
            idx_prev, g_prev = valid_gaps[j - 1]
            if (g_prev < 0 and g_now >= 0):
                cross_happened = True
                cross_days_ago = len(prices) - 1 - idx_now
                cross_type = "Golden cross"
                break
            elif (g_prev > 0 and g_now <= 0):
                cross_happened = True
                cross_days_ago = len(prices) - 1 - idx_now
                cross_type = "Death cross"
                break

    rules.append({
        "name": "Cross just happened",
        "type": "event", "weight": "major",
        "active": cross_happened,
        "detail": f"{cross_type} {cross_days_ago}d ago" if cross_happened else f"No cross in last 14d — gap at {gap:+.1f}%",
        "context": "A confirmed cross is the primary trend change signal.",
    })

    # ── Rule 2: Cross imminent (momentum, major) ──
    # Active if gap is narrowing toward zero and days-to-cross < 30
    cross_imminent = False
    days_to_cross = None
    if sl_gap_now is not None and sl_gap_now != 0:
        # Gap narrowing toward zero means: gap positive & slope negative, or gap negative & slope positive
        narrowing = (gap > 0 and sl_gap_now < 0) or (gap < 0 and sl_gap_now > 0)
        if narrowing:
            # days_to_cross = abs(gap) / abs(sl_gap_now) * 5  (slope is per 5 days)
            dtc = abs(gap) / abs(sl_gap_now) * 5
            if dtc < 30:
                cross_imminent = True
                days_to_cross = round(dtc)

    imminent_type = "golden" if gap < 0 else "death"
    rules.append({
        "name": "Cross imminent",
        "type": "momentum", "weight": "major",
        "active": cross_imminent,
        "detail": f"~{days_to_cross}d to {imminent_type} cross at current rate, gap at {gap:+.1f}%" if cross_imminent else f"Gap at {gap:+.1f}%, {'narrowing' if sl_gap_now and ((gap > 0 and sl_gap_now < 0) or (gap < 0 and sl_gap_now > 0)) else 'widening'}",
        "context": "Gap compressing fast — cross risk within a month.",
    })

    # ── Rule 3: 50d MA inflected (event, minor) ──
    # Active if 50d slope changed sign in last 7 days with 3-day persistence before flip
    ma50_inflected = False
    ma50_dir = None
    ma50_days_ago = None
    sl50_valid = [(i, s) for i, s in enumerate(sl50) if s is not None]
    if len(sl50_valid) >= 5:
        for j in range(len(sl50_valid) - 1, max(len(sl50_valid) - 8, 0), -1):
            idx_now, s_now = sl50_valid[j]
            idx_prev, s_prev = sl50_valid[j - 1]
            if (s_prev < 0 and s_now >= 0) or (s_prev > 0 and s_now <= 0):
                days_ago = len(prices) - 1 - idx_now
                if days_ago > 7:
                    break
                # Persistence check: was slope consistently in prior direction for 3+ days before flip
                prior_consistent = 0
                for k in range(j - 1, max(j - 5, 0), -1):
                    if sl50_valid[k][1] is not None:
                        if (s_prev < 0 and sl50_valid[k][1] < 0) or (s_prev > 0 and sl50_valid[k][1] > 0):
                            prior_consistent += 1
                        else:
                            break
                if prior_consistent >= 3:
                    ma50_inflected = True
                    ma50_dir = "up" if s_now >= 0 else "down"
                    ma50_days_ago = days_ago
                break

    sl50_now = _last_valid(sl50)
    rules.append({
        "name": "50d MA inflected",
        "type": "event", "weight": "minor",
        "active": ma50_inflected,
        "detail": f"50d turned {ma50_dir} {ma50_days_ago}d ago" if ma50_inflected else f"50d slope at {sl50_now:+.2f}%/5d" if sl50_now else "—",
        "context": "Short-term momentum shift. First sign of a potential trend change.",
    })

    # ── Rule 4: Gap historically extreme (structure, minor) ──
    # Active if current gap > 1.5 std from 2-year trailing mean
    gap_extreme = False
    gap_z = None
    valid_gap_vals = [g for g in gap_series if g is not None]
    trailing = valid_gap_vals[-730:] if len(valid_gap_vals) >= 365 else None
    if trailing:
        mean_gap = sum(trailing) / len(trailing)
        std_gap = math.sqrt(sum((g - mean_gap) ** 2 for g in trailing) / len(trailing))
        if std_gap > 0:
            gap_z = (gap - mean_gap) / std_gap
            gap_extreme = abs(gap_z) > 1.5

    if gap_extreme and gap_z is not None:
        direction = "above" if gap_z > 0 else "below"
        detail = f"Gap at {gap:+.1f}% — {abs(gap_z):.1f} std {direction} 2yr mean"
    else:
        detail = f"Gap at {gap:+.1f}%" + (f" ({abs(gap_z):.1f} std)" if gap_z else "")

    rules.append({
        "name": "Gap historically extreme",
        "type": "structure", "weight": "minor",
        "active": gap_extreme,
        "detail": detail,
        "context": "Gap at a historical extreme — mean reversion or trend exhaustion likely.",
    })

    # ── Rule 5: Price/structure contradiction (structure, major) ──
    # Active when price and MA structure disagree
    golden_cross = m50 > m200
    price_below_both = price < m50 and price < m200
    price_above_both = price > m50 and price > m200
    contradiction = (golden_cross and price_below_both) or (not golden_cross and price_above_both)

    if golden_cross and price_below_both:
        contra_detail = f"Golden cross but price below both MAs (price ${price:,.0f}, 50d ${m50:,.0f}, 200d ${m200:,.0f})"
    elif not golden_cross and price_above_both:
        contra_detail = f"Death cross but price above both MAs (price ${price:,.0f}, 50d ${m50:,.0f}, 200d ${m200:,.0f})"
    else:
        contra_detail = f"Price and structure aligned — {'bullish' if golden_cross and price_above_both else 'bearish' if not golden_cross and price_below_both else 'mixed'}"

    rules.append({
        "name": "Price/structure contradiction",
        "type": "structure", "weight": "major",
        "active": contradiction,
        "detail": contra_detail,
        "context": "Price and MA structure disagree — possible false cross or key retest level.",
    })

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
