"""
api/data_status.py — Returns status of all database tables
"""
from datetime import datetime, timezone
from api.shared import get_conn
import psycopg2.extras


def handle_data_status(params):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    tables = [
        {"name": "Price", "table": "price_daily", "granularity": "Daily",
         "source": "CoinGecko", "category": "market_data",
         "count_col": "coingecko_id"},
        {"name": "Price (Hourly)", "table": "price_hourly", "granularity": "Hourly",
         "source": "CoinGecko", "category": "market_data",
         "count_col": "coingecko_id"},
        {"name": "Market Cap", "table": "marketcap_daily", "granularity": "Daily",
         "source": "CoinGecko", "category": "market_data",
         "count_col": "coingecko_id"},
        {"name": "Volume", "table": "volume_daily", "granularity": "Daily",
         "source": "CoinGecko", "category": "market_data",
         "count_col": "coingecko_id"},
        {"name": "Total Crypto Mcap", "table": "total_marketcap_daily", "granularity": "Daily",
         "source": "CoinGecko", "category": "market_data",
         "count_col": None},
        {"name": "Alt Intracorrelation", "table": "alt_intracorr_daily", "granularity": "Daily",
         "source": "Computed", "category": "market_data",
         "count_col": "tier"},
        {"name": "Funding Rate", "table": "funding_8h", "granularity": "8h",
         "source": "Binance/Bybit", "category": "perps",
         "count_col": "symbol"},
        {"name": "Open Interest (Daily)", "table": "open_interest_daily", "granularity": "Daily",
         "source": "Binance/Bybit", "category": "perps",
         "count_col": "symbol"},
        {"name": "Open Interest (Hourly)", "table": "open_interest_hourly", "granularity": "Hourly",
         "source": "Binance/Bybit", "category": "perps",
         "count_col": "symbol"},
        {"name": "Long/Short Ratio", "table": "long_short_ratio", "granularity": "Daily/1h",
         "source": "Binance/Bybit", "category": "perps",
         "count_col": "symbol"},
        {"name": "DVOL (Implied Vol)", "table": "dvol_daily", "granularity": "Daily",
         "source": "Deribit", "category": "derivatives",
         "count_col": "currency"},
        {"name": "Options (BTC/ETH)", "table": "options_daily", "granularity": "Daily",
         "source": "Deribit", "category": "derivatives",
         "count_col": "currency"},
        {"name": "Options Instruments", "table": "options_instruments_daily", "granularity": "Daily",
         "source": "Deribit", "category": "derivatives",
         "count_col": "currency"},
        {"name": "ETF AUM", "table": "etf_aum_daily", "granularity": "Daily",
         "source": "yfinance", "category": "etf",
         "count_col": "ticker"},
        {"name": "ETF Flows", "table": "etf_flows_daily", "granularity": "Daily",
         "source": "Farside", "category": "etf",
         "count_col": "ticker"},
        {"name": "Macro Assets (Daily)", "table": "macro_daily", "granularity": "Daily",
         "source": "yfinance", "category": "misc",
         "count_col": "ticker"},
        {"name": "Macro Assets (Hourly)", "table": "macro_hourly", "granularity": "Hourly",
         "source": "yfinance", "category": "misc",
         "count_col": "ticker"},
        {"name": "On-chain (BTC)", "table": "onchain_daily", "granularity": "Daily",
         "source": "CoinMetrics", "category": "misc",
         "count_col": "metric"},
        {"name": "Asset Registry", "table": "asset_registry", "granularity": "Static",
         "source": "Internal", "category": "misc",
         "count_col": None, "no_timestamp": True},
    ]

    results = []
    now = datetime.now(timezone.utc)

    for t in tables:
        try:
            # Row count
            cur.execute(f"SELECT COUNT(*) FROM {t['table']}")
            row_count = cur.fetchone()['count']

            # Asset count
            if t.get('count_col'):
                cur.execute(f"SELECT COUNT(DISTINCT {t['count_col']}) FROM {t['table']}")
                asset_count = cur.fetchone()['count']
            elif t.get('no_timestamp'):
                cur.execute(f"SELECT COUNT(*) FROM {t['table']}")
                asset_count = cur.fetchone()['count']
            else:
                asset_count = 1

            # Date range + last updated
            if not t.get('no_timestamp'):
                cur.execute(f"SELECT MIN(timestamp)::date, MAX(timestamp)::date FROM {t['table']}")
                r = cur.fetchone()
                date_from = str(r['min']) if r['min'] else None
                date_to = str(r['max']) if r['max'] else None

                # Check staleness
                if date_to:
                    last_dt = datetime.strptime(date_to, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    days_stale = (now - last_dt).days
                    if t['granularity'] == 'Hourly':
                        status = 'live' if days_stale <= 2 else 'stale'
                    elif t['granularity'] == '8h':
                        status = 'live' if days_stale <= 2 else 'stale'
                    else:
                        status = 'live' if days_stale <= 3 else 'stale'
                else:
                    status = 'empty'
            else:
                date_from = None
                date_to = None
                status = 'static'

            results.append({
                "name": t['name'],
                "table": t['table'],
                "granularity": t['granularity'],
                "source": t['source'],
                "category": t['category'],
                "assets": asset_count,
                "rows": row_count,
                "date_from": date_from,
                "date_to": date_to,
                "status": status,
            })
        except Exception as e:
            results.append({
                "name": t['name'],
                "table": t['table'],
                "granularity": t['granularity'],
                "source": t['source'],
                "category": t['category'],
                "assets": 0, "rows": 0,
                "date_from": None, "date_to": None,
                "status": "error",
                "error": str(e),
            })

    conn.close()
    return {"updated": now.strftime("%Y-%m-%d %H:%M UTC"), "datasets": results}
