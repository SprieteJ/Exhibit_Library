#!/usr/bin/env python3
"""
etf_flows_backfill.py v2 — with better headers to avoid 403
"""

import os, re, requests, psycopg2, psycopg2.extras
import pandas as pd
from datetime import datetime, timezone
from io import StringIO

DATABASE_URL = os.environ["DATABASE_URL"]

SOURCES = {
    "BTC": [
        "https://farside.co.uk/bitcoin-etf-flow-all-data/",
        "https://farside.co.uk/btc/",
    ],
    "ETH": [
        "https://farside.co.uk/ethereum-etf-flow-all-data/",
        "https://farside.co.uk/eth/",
    ],
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def bulk_upsert(df):
    if df.empty: return 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TEMP TABLE _tmp (LIKE etf_flows_daily INCLUDING DEFAULTS) ON COMMIT DROP")
        buf = StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cols = list(df.columns)
        cur.copy_from(buf, "_tmp", columns=cols, null="\\N")
        col_str = ", ".join(cols)
        update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in ["flow_usd_m", "source", "ingested_at"])
        cur.execute(f"INSERT INTO etf_flows_daily ({col_str}) SELECT {col_str} FROM _tmp ON CONFLICT (timestamp, ticker, asset) DO UPDATE SET {update_str}")
        n = cur.rowcount
        conn.commit()
        return n
    except Exception as e:
        conn.rollback()
        print(f"  DB error: {e}")
        return 0
    finally:
        conn.close()


def parse_flow_value(val):
    if pd.isna(val): return None
    s = str(val).strip()
    if not s or s in ['-', '—', '', 'N/A', 'n/a', 'Holiday', 'holiday']: return None
    s = s.replace(',', '')
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def fetch_page(urls):
    for url in urls:
        try:
            print(f"  Trying {url}...", end=" ")
            resp = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
            if resp.status_code == 200:
                print(f"OK ({len(resp.text)} bytes)")
                return resp.text
            else:
                print(f"{resp.status_code}")
        except Exception as e:
            print(f"Error: {e}")
    return None


def scrape_farside(asset, urls):
    html_text = fetch_page(urls)
    if not html_text:
        print(f"  Could not fetch any URL for {asset}")
        return pd.DataFrame()

    try:
        tables = pd.read_html(html_text)
    except Exception as e:
        print(f"  Failed to parse HTML tables: {e}")
        return pd.DataFrame()

    print(f"  Found {len(tables)} tables")

    all_rows = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for df in tables:
        if df.empty or len(df.columns) < 3: continue
        date_col = df.columns[0]

        for _, row in df.iterrows():
            date_val = row[date_col]
            if pd.isna(date_val): continue
            date_str = str(date_val).strip()
            if any(skip in date_str.lower() for skip in ['total', 'date', 'week', 'month', 'year', 'average', 'unnamed']): continue

            parsed_date = None
            for fmt in ["%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%d-%b-%y", "%d %b %y"]:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
            if not parsed_date: continue
            ts = parsed_date.strftime("%Y-%m-%d")

            for col in df.columns[1:]:
                col_name = str(col).strip().upper()
                if any(skip in col_name for skip in ['TOTAL', 'UNNAMED', 'EXCL']): continue
                clean_col = re.sub(r'[^A-Z0-9]', '', col_name)
                if not clean_col or len(clean_col) > 10: continue

                flow = parse_flow_value(row[col])
                if flow is not None:
                    all_rows.append({"timestamp": ts, "ticker": clean_col, "asset": asset,
                                     "flow_usd_m": round(flow, 2), "source": "farside", "ingested_at": now})

    if all_rows:
        result = pd.DataFrame(all_rows)
        result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
        result = result.drop_duplicates(subset=["timestamp", "ticker", "asset"])
        print(f"  Parsed {len(result)} flow records")
        return result
    print("  No flow records parsed")
    return pd.DataFrame()


def main():
    print("━" * 60)
    print("  ETF FLOWS BACKFILL (Farside Investors)")
    print("━" * 60)
    total = 0
    for asset, urls in SOURCES.items():
        print(f"\n  [{asset}]")
        df = scrape_farside(asset, urls)
        if not df.empty:
            n = bulk_upsert(df)
            total += n
            tickers = sorted(df["ticker"].unique())
            print(f"  {asset}: +{n} rows ({df['timestamp'].min().date()} to {df['timestamp'].max().date()})")
            print(f"    Tickers: {', '.join(tickers)}")
    print(f"\n{'━' * 60}")
    print(f"  DONE — {total} total rows upserted")
    print(f"{'━' * 60}")

if __name__ == "__main__":
    main()
