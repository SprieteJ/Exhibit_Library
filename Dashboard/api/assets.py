"""
api/assets.py — asset list + DB status endpoints
"""
from datetime import datetime, timezone
from .shared import get_conn, MAJORS, MACRO_TICKERS
import psycopg2.extras


def handle_assets(params):
    tab  = params.get("tab", ["individual"])[0]
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if tab in ("majors", "bitcoin"):
        cur.execute("""
            SELECT DISTINCT p.symbol, r.coingecko_name as name
            FROM price_daily p
            LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
            WHERE p.symbol = ANY(%s) ORDER BY p.symbol
        """, (MAJORS,))
    elif tab == "altcoins":
        cur.execute("""
            SELECT DISTINCT p.symbol, r.coingecko_name as name
            FROM price_daily p
            LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
            WHERE p.symbol != ALL(%s) ORDER BY p.symbol
        """, (MAJORS,))
    elif tab == "macro":
        cur.execute("""
            SELECT DISTINCT ticker as symbol, name FROM macro_daily
            WHERE ticker = ANY(%s) ORDER BY ticker
        """, (MACRO_TICKERS,))
    else:
        cur.execute("""
            SELECT DISTINCT p.symbol, r.coingecko_name as name
            FROM price_daily p
            LEFT JOIN asset_registry r ON p.coingecko_id = r.coingecko_id
            ORDER BY p.symbol
        """)

    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def handle_db_status():
    TABLES = [
        {"key": "price_daily",         "label": "Price",                    "granularity": "Daily",    "source": "CoinGecko",     "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "price_hourly",        "label": "Price",                    "granularity": "Hourly",   "source": "CoinGecko",     "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "marketcap_daily",     "label": "Market cap",               "granularity": "Daily",    "source": "CoinGecko",     "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "volume_daily",        "label": "Volume",                   "granularity": "Daily",    "source": "CoinGecko",     "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "funding_8h",          "label": "Funding rate",             "granularity": "8h",       "source": "Binance/Bybit", "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "open_interest_daily", "label": "Open interest",            "granularity": "Daily",    "source": "Binance/Bybit", "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "open_interest_hourly","label": "Open interest",            "granularity": "Hourly",   "source": "Binance/Bybit", "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "long_short_ratio",    "label": "Long/short ratio",         "granularity": "Daily/1h", "source": "Binance/Bybit", "asset_col": "coingecko_id", "ts_col": "timestamp"},
        {"key": "macro_daily",         "label": "Macro assets",             "granularity": "Daily",    "source": "yfinance",      "asset_col": "ticker",       "ts_col": "timestamp"},
        {"key": "macro_hourly",        "label": "Macro assets",             "granularity": "Hourly",   "source": "yfinance",      "asset_col": "ticker",       "ts_col": "timestamp"},
        {"key": "asset_registry",      "label": "GMCI asset classification","granularity": "Static",   "source": "Internal",      "asset_col": "symbol",       "ts_col": None},
    ]

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    now  = datetime.now(timezone.utc)
    result = []

    for t in TABLES:
        try:
            if t["ts_col"]:
                cur.execute(f"""
                    SELECT COUNT(*) as rows,
                           COUNT(DISTINCT {t["asset_col"]}) as assets,
                           MIN({t["ts_col"]})::date as date_from,
                           MAX({t["ts_col"]})::date as date_to,
                           MAX(ingested_at) as last_updated
                    FROM {t["key"]}
                """)
            else:
                cur.execute(f"""
                    SELECT COUNT(*) as rows,
                           COUNT(DISTINCT {t["asset_col"]}) as assets,
                           NULL as date_from, NULL as date_to, NULL as last_updated
                    FROM {t["key"]}
                """)
            row = cur.fetchone()
            lu  = row["last_updated"]
            if lu is None:
                status = "manual"
            else:
                if lu.tzinfo is None: lu = lu.replace(tzinfo=timezone.utc)
                status = "live" if (now - lu).total_seconds() / 3600 <= 48 else "stale"
                lu = lu.strftime("%Y-%m-%d %H:%M")

            result.append({
                "label":        t["label"],
                "granularity":  t["granularity"],
                "source":       t["source"],
                "rows":         int(row["rows"]),
                "assets":       int(row["assets"]),
                "date_from":    str(row["date_from"]) if row["date_from"] else "—",
                "date_to":      str(row["date_to"])   if row["date_to"]   else "—",
                "last_updated": lu if lu else "—",
                "status":       status,
            })
        except Exception as e:
            result.append({
                "label": t["label"], "granularity": t["granularity"], "source": t["source"],
                "rows": 0, "assets": 0, "date_from": "—", "date_to": "—",
                "last_updated": "—", "status": "error", "error": str(e),
            })

    conn.close()
    return result
