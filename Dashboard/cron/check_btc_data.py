#!/usr/bin/env python3
"""
Quick diagnostic: what BTC funding + OI data do we actually have?
Run: DATABASE_URL="your_public_url" python3 check_btc_data.py
"""
import os, psycopg2, psycopg2.extras

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

print("=" * 70)
print("FUNDING_8H — BTC")
print("=" * 70)
cur.execute("""
    SELECT exchange, symbol, MIN(timestamp) as earliest, MAX(timestamp) as latest, COUNT(*) as rows
    FROM funding_8h
    WHERE symbol = 'BTC'
    GROUP BY exchange, symbol
    ORDER BY exchange
""")
for r in cur.fetchall():
    print(f"  {r['exchange']:10s} | {r['earliest']} → {r['latest']} | {r['rows']} rows")

# Also check if BTC is stored differently (e.g. as coingecko_id 'bitcoin')
print("\nFUNDING_8H — checking coingecko_id = 'bitcoin'")
cur.execute("""
    SELECT exchange, coingecko_id, symbol, MIN(timestamp) as earliest, MAX(timestamp) as latest, COUNT(*) as rows
    FROM funding_8h
    WHERE coingecko_id = 'bitcoin'
    GROUP BY exchange, coingecko_id, symbol
    ORDER BY exchange
""")
rows = cur.fetchall()
if not rows:
    print("  (no rows)")
else:
    for r in rows:
        print(f"  {r['exchange']:10s} | cg_id={r['coingecko_id']} sym={r['symbol']} | {r['earliest']} → {r['latest']} | {r['rows']} rows")

print("\nFUNDING_8H — all distinct symbols (first 20)")
cur.execute("SELECT DISTINCT symbol FROM funding_8h ORDER BY symbol LIMIT 20")
print(f"  {[r['symbol'] for r in cur.fetchall()]}")

print("\nFUNDING_8H — total rows per exchange")
cur.execute("SELECT exchange, COUNT(*) FROM funding_8h GROUP BY exchange")
for r in cur.fetchall():
    print(f"  {r['exchange']:10s} | {r['count']} rows")

print("\n" + "=" * 70)
print("OPEN_INTEREST_DAILY — BTC")
print("=" * 70)
cur.execute("""
    SELECT exchange, symbol, MIN(timestamp) as earliest, MAX(timestamp) as latest, COUNT(*) as rows
    FROM open_interest_daily
    WHERE symbol = 'BTC'
    GROUP BY exchange, symbol
    ORDER BY exchange
""")
for r in cur.fetchall():
    print(f"  {r['exchange']:10s} | {r['earliest']} → {r['latest']} | {r['rows']} rows")

print("\nOPEN_INTEREST_DAILY — sample BTC rows (last 5 per exchange)")
cur.execute("""
    (SELECT exchange, timestamp, symbol, oi_usd, oi_contracts
     FROM open_interest_daily WHERE symbol = 'BTC' AND exchange = 'binance'
     ORDER BY timestamp DESC LIMIT 5)
    UNION ALL
    (SELECT exchange, timestamp, symbol, oi_usd, oi_contracts
     FROM open_interest_daily WHERE symbol = 'BTC' AND exchange = 'bybit'
     ORDER BY timestamp DESC LIMIT 5)
    ORDER BY exchange, timestamp DESC
""")
for r in cur.fetchall():
    print(f"  {r['exchange']:10s} | {r['timestamp']} | oi_usd={r['oi_usd']} | oi_contracts={r['oi_contracts']}")

print("\n" + "=" * 70)
print("OPEN_INTEREST_DAILY — total rows per exchange")
print("=" * 70)
cur.execute("SELECT exchange, COUNT(*), COUNT(oi_usd) as has_usd, COUNT(oi_contracts) as has_contracts FROM open_interest_daily GROUP BY exchange")
for r in cur.fetchall():
    print(f"  {r['exchange']:10s} | {r['count']} total | {r['has_usd']} with oi_usd | {r['has_contracts']} with oi_contracts")

conn.close()
print("\nDone.")
