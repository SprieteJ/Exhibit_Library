# Wintermute Dashboard — Complete Reference (Updated 2026-04-01)

> **For Claude Code**: This document contains everything needed to modify, extend, and maintain the Wintermute crypto analytics dashboard.

---

## 1. Database Connection

```
# Public (for local scripts)
DATABASE_URL = postgresql://postgres:SUebOdInrQhCQZFXJJxAzkBNXUganSSR@centerbeam.proxy.rlwy.net:41494/railway

# Internal (for Railway services)
DATABASE_URL = postgresql://postgres:SUebOdInrQhCQZFXJJxAzkBNXUganSSR@postgres.railway.internal:5432/railway
```

**API Keys:**
- `COINGECKO_API_KEY`: `CG-jrgUr1nTKsJh6yjWJeLrYaWM` (Pro tier)
- Deribit: no key needed (public API)
- Binance/Bybit: no keys needed (public APIs, but Binance blocks US IPs — Railway EU region works)

---

## 2. Database Schema (13 tables)

### asset_registry (584 rows, static)
Primary key: `symbol`. Links to all crypto tables via `coingecko_id`.
Columns: symbol, market_cap, sector, use_case, sub_use_case, tech_stack, sub_tech_stack, tertiary_tech, ecosystem, region, custodies, coingecko_id, coingecko_symbol, coingecko_name, cmc_id, cmc_symbol, cmc_name, cmc_slug, coinmetrics_id, coinmetrics_name, registry_version

### price_daily — UNIQUE(timestamp, coingecko_id)
timestamp (timestamptz), coingecko_id (text), symbol (text), price_usd (float8), source (text), ingested_at (timestamptz)
Coverage: 2013-04-28 to present, ~900k rows

### price_hourly — UNIQUE(timestamp, coingecko_id)
Same schema as price_daily. Coverage: 2025-10-06 to present, ~1.25M rows

### marketcap_daily — UNIQUE(timestamp, coingecko_id)
timestamp, coingecko_id, symbol, market_cap_usd (float8), source, ingested_at
Coverage: 2013-04-28 to present, ~850k rows

### volume_daily — UNIQUE(timestamp, coingecko_id)
timestamp, coingecko_id, symbol, volume_usd (float8), source, ingested_at
Coverage: 2013-12-27 to present, ~900k rows

### total_marketcap_daily — UNIQUE(timestamp)
timestamp (timestamptz), total_mcap_usd (float8), source (text), ingested_at (timestamptz)
Coverage: 2013-04-29 to present, ~4,700 rows. Source: CoinGecko /global/market_cap_chart

### funding_8h — UNIQUE(timestamp, coingecko_id, exchange)
timestamp, coingecko_id, symbol, exchange ("binance"/"bybit"), funding_rate (float8), source, ingested_at
To annualise: funding_rate * 3 * 365 * 100

### open_interest_daily — UNIQUE(timestamp, coingecko_id, exchange)
timestamp, coingecko_id, symbol, exchange, oi_usd (float8, NULL for Bybit), oi_contracts (float8), source, ingested_at
Note: Bybit lacks oi_usd — compute as oi_contracts * price

### open_interest_hourly — UNIQUE(timestamp, coingecko_id, exchange)
Same schema as daily. ~7.65M rows

### long_short_ratio — UNIQUE(timestamp, coingecko_id, exchange, period)
timestamp, coingecko_id, symbol, exchange, ls_ratio (float8), period ("1d"/"1h"), buy_ratio, sell_ratio, source, ingested_at

### macro_daily — UNIQUE(timestamp, ticker)
timestamp, ticker (text), name (text), asset_class (text), open, high, low, close (float8), volume (bigint), source, ingested_at
29 tickers: SPY, QQQ, IWM, DIA, ^VIX, TLT, IEF, SHY, ^TNX, ^IRX, ^TYX, GLD, SLV, BNO, USO, NG=F, DX-Y.NYB, EURUSD=X, JPYUSD=X, IBIT, FBTC, ARKB, BITB, ETHA, ETHW, MSTR, COIN, MARA, RIOT

### macro_hourly — UNIQUE(timestamp, ticker)
Same schema. ~60 rolling days of data.

### dvol_daily — UNIQUE(timestamp, currency)
timestamp (timestamptz), currency ("BTC"/"ETH"), open, high, low, close (float8), source, ingested_at
Coverage: 2021-03-01 to present, ~3,666 rows. Source: Deribit public/get_volatility_index_data

---

## 3. Repository Structure

```
Exhibit_Library/
  Dashboard/
    app.py                    # HTTP router (~170 lines)
    index.html                # Frontend SPA (~4000+ lines)
    api/
      __init__.py
      shared.py               # get_conn, rebase_series, rolling_corr, sector helpers
      sector.py               # 18 sector chart handlers
      bitcoin.py              # 18 BTC chart handlers
      ethereum.py             # 6 ETH chart handlers
      altcoins.py             # 8 altcoin handlers
      alt_market.py           # 7 alt market-wide handlers
      macro.py                # 6 macro handlers
      crypto_market.py        # 1 handler (total mcap with MAs)
      control_center.py       # Signal matrix with 19 indicators
      assets.py               # Asset list + DB status
      price.py                # Generic price handler
    cron/
      daily_update.py         # Daily incremental updater
      dvol_backfill.py
      total_mcap_backfill.py
      Dockerfile.cron
      requirements-cron.txt
      railway.toml
```

---

## 4. All Current API Endpoints (75 total)

### General
/api/assets, /api/db-status, /api/latest-date, /api/price, /api/control-center, /api/total-mcap

### Sectors (18)
/api/sectors, /api/sector-price, /api/sector-mcap, /api/sector-intra-corr, /api/sector-btc-corr, /api/sector-momentum, /api/sector-zscore, /api/sector-bubble, /api/sector-mcap-view, /api/sector-rrg, /api/sector-dominance, /api/sector-xheatmap, /api/sector-cumulative, /api/sector-vol, /api/sector-drawdown, /api/sector-breadth, /api/sector-funding, /api/sector-oi, /api/sector-sharpe

### Bitcoin (18)
/api/btc-epochs, /api/btc-cycles, /api/btc-gold, /api/btc-rolling, /api/btc-bull, /api/btc-realvol, /api/btc-drawdown, /api/btc-gold-ratio, /api/btc-dominance, /api/btc-funding, /api/btc-oi, /api/btc-funding-delta, /api/btc-ma, /api/btc-200w-floor, /api/btc-200d-dev, /api/btc-ma-gap, /api/btc-pi-cycle, /api/btc-mcap, /api/btc-rv-iv

### Ethereum (6)
/api/eth-ma, /api/eth-ma-gap, /api/eth-200d-dev, /api/eth-drawdown, /api/eth-mcap, /api/eth-btc-ratio

### Altcoins (8)
/api/alt-scatter, /api/alt-altseason, /api/alt-beta, /api/alt-heatmap, /api/alt-ath-drawdown, /api/alt-funding-heatmap, /api/alt-drawdown-ts

### Alt Market (7)
/api/alt-mcap-total, /api/alt-mcap-gap, /api/alt-mcap-dev, /api/dominance-shares, /api/alt-rel-share, /api/btc-alt-ratio, /api/alt-intracorr

### Macro (6)
/api/macro-price, /api/macro-matrix, /api/macro-dxy-btc, /api/macro-risk, /api/macro-real-yields, /api/macro-stablecoin

---

## 5. Frontend Patterns

### Tab definition
```javascript
const TABS = {
  tab_name: {
    label: "Display Name",
    groups: [
      { label: "Group", children: [
        { key: "view-key", label: "Label", sub: "Description", src: "Source" },
      ]},
    ]
  },
};
```

### Auto-view registration (views that load without sidebar selection)
Add key to both `AUTO_VIEWS` Set and `NEW_AUTO_KEYS` Set, then add switch case in fetchCurrent().

### Chart function pattern
```javascript
async function fetchMyChart(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/endpoint?from=${from}&to=${to}`).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [...] }, { scales: {...}, plugins: {...} });
    setTitle('Title', 'Source: ...');
  } catch(e) { showErr(e); }
}
```

### Available JS helpers
PAL (color array), XTICK/YTICK (tick styles), XGRID/YGRID (grid styles), spinOn(), showErr(e), mkChart(type, data, opts), setTitle(title, sub)

### Control center renders HTML table (not chart)
Destroys chart, hides canvas, creates div#cc-matrix-container with styled table. mkChart removes this div automatically when switching to a chart view.

---

## 6. Control Center Signal Structure

```python
{
    "id":         "ma-cross",
    "name":       "50d / 200d MA Cross",
    "chart_name": "MA Gap",              # clickable link text
    "chart_tab":  "bitcoin",             # tab to navigate to
    "chart_key":  "btc-ma-gap",          # view key
    "group":      "Moving Averages",     # grouping
    "status":     "green",               # green/yellow/red/grey
    "trend":      "down",                # up/down/flat
    "detail":     "50d MA 2.3% above",   # current reading
    "context":    "Golden cross = ..."   # why it matters
}
```

Frontend has STATUS_THRESHOLDS object mapping signal IDs to hover tooltip text on status dots.

---

## 7. Hosting & Deployment

Platform: Railway. Dashboard = always-on web, Cron = scheduled EU region.
Repo: github.com/SprieteJ/Exhibit_Library, branch main.
Deploy: git push to main auto-deploys.
Python driver: psycopg2-binary. All timestamps UTC.
