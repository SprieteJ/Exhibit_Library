# Wintermute Dashboard — Daily Updater Setup
## Railway Cron Service

### What this does
A single Python script (`daily_update.py`) that runs once daily and:
1. Queries each Postgres table for the latest timestamp per asset
2. Fetches only the missing data from CoinGecko / Binance / Bybit / yfinance
3. Upserts into Postgres (ON CONFLICT DO NOTHING — safe to re-run)

### Files to add to your repo

Place these in `Dashboard/cron/`:
```
Dashboard/
├── cron/
│   ├── Dockerfile.cron
│   ├── requirements-cron.txt
│   └── daily_update.py
├── app.py
├── api/
│   └── ...
└── index.html
```

### Railway setup (one-time)

1. **Go to your Railway project** (the one with your dashboard + Postgres)

2. **Add a new service:**
   - Click "New" → "GitHub Repo" → select your Exhibit_Library repo
   - Or click "New" → "Empty Service" and connect it manually

3. **Configure the service:**
   - **Root Directory:** `Dashboard/cron`
   - **Dockerfile Path:** `Dockerfile.cron`
   - **Start Command:** (leave empty, Dockerfile handles it)

4. **Set the cron schedule:**
   - Go to service Settings → Deploy → Cron Schedule
   - Set to: `0 6 * * *` (runs at 06:00 UTC daily)
   - This gives CoinGecko/yfinance time to settle their daily candles

5. **Environment variables:**
   - `DATABASE_URL` — use the same reference variable from your Postgres service
     (Railway lets you reference another service's variables: `${{Postgres.DATABASE_URL}}`)
   - `COINGECKO_API_KEY` — your Pro API key

6. **Deploy.** Railway will build the Docker image and run it on schedule.

### Running manually

From the Railway dashboard, you can trigger the cron manually via the
"Deploy" button on the cron service. Or SSH in and run:

```bash
python daily_update.py           # all tables
python daily_update.py prices    # just CoinGecko (price + mcap + volume)
python daily_update.py derivatives  # just Binance/Bybit
python daily_update.py macro     # just yfinance macro
```

### How the gap-filling works

For each asset in each table, the updater:
1. Runs `SELECT MAX(timestamp) FROM table WHERE coingecko_id = 'xxx'`
2. Calculates `gap_days = today - last_date`
3. If gap ≤ 1 day → skip (already up to date)
4. If gap > 1 day → fetch that many days from the API
5. Filter out today's partial candle
6. INSERT ... ON CONFLICT DO NOTHING

This means:
- If the cron misses a day → next run auto-backfills
- If you re-run the same day → no duplicates, no wasted API calls
- New assets added to the registry get picked up automatically (fetches last 90 days)

### CoinGecko optimisation

The `update_coingecko_combined()` function fetches price, market_cap, and volume
from a single `/coins/{id}/market_chart` call (the endpoint returns all three).
This cuts API usage by ~3x compared to calling each separately.

For 584 assets at 0.25s sleep between calls, a full pass takes ~2.5 minutes.
Most days, the majority will be skipped (gap ≤ 1), so typical runtime is much faster.

### Estimated daily runtime

| Source       | Assets | Typical runtime |
|-------------|--------|-----------------|
| CoinGecko   | 584    | 2-5 min         |
| Binance     | ~350   | 5-10 min        |
| Bybit       | ~300   | 5-10 min        |
| yfinance    | 29     | 1-2 min         |
| **Total**   |        | **~15-25 min**  |

### Prerequisite: unique constraints

The ON CONFLICT upsert requires unique constraints on your Postgres tables.
If you haven't already set these up, run this once:

```sql
-- Price tables
ALTER TABLE price_daily ADD CONSTRAINT price_daily_pkey
    UNIQUE (timestamp, coingecko_id);
ALTER TABLE price_hourly ADD CONSTRAINT price_hourly_pkey
    UNIQUE (timestamp, coingecko_id);

-- Market cap
ALTER TABLE marketcap_daily ADD CONSTRAINT marketcap_daily_pkey
    UNIQUE (timestamp, coingecko_id);

-- Volume
ALTER TABLE volume_daily ADD CONSTRAINT volume_daily_pkey
    UNIQUE (timestamp, coingecko_id);

-- Funding rates
ALTER TABLE funding_8h ADD CONSTRAINT funding_8h_pkey
    UNIQUE (timestamp, coingecko_id, exchange);

-- Open interest
ALTER TABLE open_interest_daily ADD CONSTRAINT oi_daily_pkey
    UNIQUE (timestamp, coingecko_id, exchange);
ALTER TABLE open_interest_hourly ADD CONSTRAINT oi_hourly_pkey
    UNIQUE (timestamp, coingecko_id, exchange);

-- Long/short ratio
ALTER TABLE long_short_ratio ADD CONSTRAINT ls_ratio_pkey
    UNIQUE (timestamp, coingecko_id, exchange, period);

-- Macro
ALTER TABLE macro_daily ADD CONSTRAINT macro_daily_pkey
    UNIQUE (timestamp, ticker);
ALTER TABLE macro_hourly ADD CONSTRAINT macro_hourly_pkey
    UNIQUE (timestamp, ticker);
```

Run these in the Railway Postgres Data tab before your first cron run.
If any constraint already exists, the ALTER will just throw a harmless error.
