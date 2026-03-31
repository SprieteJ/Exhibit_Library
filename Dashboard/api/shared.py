"""
api/shared.py — shared helpers used across all API modules
"""
import os, math, json
from pathlib import Path
from collections import defaultdict
import psycopg2, psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL       = os.environ.get("DATABASE_URL", "")
BASE_DIR     = Path(__file__).parent.parent
SECTORS_FILE = BASE_DIR / "sectors.json"

def load_sectors():
    if SECTORS_FILE.exists():
        with open(SECTORS_FILE) as f: return json.load(f)
    return {}

SECTORS = load_sectors()

SECTOR_COLORS = {
    "Memecoins":   "#F7931A",
    "Layer 1":     "#2471CC",
    "Layer 2":     "#746BE6",
    "DeFi":        "#00D64A",
    "DePIN":       "#DB33CB",
    "Gaming":      "#EC5B5B",
    "AI":          "#26A17B",
    "Stablecoins": "#888B88",
}

MAJORS        = ["BTC", "ETH"]
MACRO_TICKERS = ["SPY","QQQ","IWM","DIA","TLT","IEF","SHY","GLD","SLV","BNO","USO",
                 "DX-Y.NYB","EURUSD=X","JPYUSD=X","^VIX","^TNX","^IRX","^TYX"]

# ── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DB_URL)

# ── Table helpers ─────────────────────────────────────────────────────────────
def price_table(g):  return "price_hourly"  if g == "hourly" else "price_daily"
def macro_table(g):  return "macro_hourly"  if g == "hourly" else "macro_daily"
def ts_cast(g):      return "timestamp"     if g == "hourly" else "timestamp::date"

# ── Maths helpers ─────────────────────────────────────────────────────────────
def rebase_series(prices):
    first = next((p for p in prices if p is not None and not math.isnan(p)), None)
    if not first or first == 0: return prices
    return [round(p / first * 100, 4) if p is not None else None for p in prices]

def rolling_corr(x, y, window):
    n = len(x)
    result = [None] * n
    for i in range(window - 1, n):
        xs = x[i-window+1:i+1]; ys = y[i-window+1:i+1]
        pairs = [(a,b) for a,b in zip(xs,ys) if a is not None and b is not None]
        if len(pairs) < window // 2: continue
        xa = sum(p[0] for p in pairs)/len(pairs)
        ya = sum(p[1] for p in pairs)/len(pairs)
        num = sum((p[0]-xa)*(p[1]-ya) for p in pairs)
        dx  = math.sqrt(sum((p[0]-xa)**2 for p in pairs))
        dy  = math.sqrt(sum((p[1]-ya)**2 for p in pairs))
        if dx > 0 and dy > 0:
            result[i] = round(num/(dx*dy), 4)
    return result

# ── Sector index builder ──────────────────────────────────────────────────────
def fetch_sector_index(cur, cg_ids, date_from, date_to, weighting='equal', granularity='daily'):
    tbl = price_table(granularity)
    if granularity == "hourly":
        cur.execute(f"""
            SELECT p.coingecko_id, p.timestamp as ts, p.price_usd,
                   NULL::double precision as market_cap_usd
            FROM {tbl} p
            WHERE p.coingecko_id = ANY(%s)
              AND p.timestamp >= %s AND p.timestamp <= %s
              AND p.price_usd > 0
            ORDER BY p.coingecko_id, p.timestamp
        """, (cg_ids, date_from, date_to))
    else:
        cur.execute(f"""
            SELECT p.coingecko_id, p.timestamp::date as ts, p.price_usd, m.market_cap_usd
            FROM {tbl} p
            LEFT JOIN marketcap_daily m
              ON p.coingecko_id = m.coingecko_id
              AND p.timestamp::date = m.timestamp::date
            WHERE p.coingecko_id = ANY(%s)
              AND p.timestamp >= %s AND p.timestamp <= %s
              AND p.price_usd > 0
            ORDER BY p.coingecko_id, p.timestamp
        """, (cg_ids, date_from, date_to))

    rows = cur.fetchall()
    if not rows: return {}, []

    asset_prices = defaultdict(dict)
    asset_mcaps  = defaultdict(dict)
    for row in rows:
        cid = row['coingecko_id']; d = str(row['ts'])
        asset_prices[cid][d] = float(row['price_usd'])
        if row['market_cap_usd']:
            asset_mcaps[cid][d] = float(row['market_cap_usd'])

    all_dates = sorted(set(d for s in asset_prices.values() for d in s))
    if not all_dates: return {}, []

    rebased = {}
    for cid, prices in asset_prices.items():
        sorted_d = sorted(prices); first = prices[sorted_d[0]]
        if first > 0:
            rebased[cid] = {d: prices[d]/first*100 for d in sorted_d}

    if not rebased: return {}, []
    min_assets = max(1, len(rebased) * 0.5)
    index = {}

    for date in all_dates:
        if weighting == 'mcap':
            vals_w = [
                (series[date], asset_mcaps.get(cid,{}).get(date,0))
                for cid, series in rebased.items()
                if date in series and asset_mcaps.get(cid,{}).get(date,0) > 0
            ]
            if len(vals_w) >= min_assets:
                tw = sum(w for _,w in vals_w)
                index[date] = round(sum(v*w/tw for v,w in vals_w), 4)
        else:
            vals = [s[date] for s in rebased.values() if date in s]
            if len(vals) >= min_assets:
                index[date] = round(sum(vals)/len(vals), 4)

    if index:
        sd = sorted(index); first = index[sd[0]]
        if first > 0:
            index = {d: round(v/first*100,4) for d,v in index.items()}

    return index, sorted(index.keys())
