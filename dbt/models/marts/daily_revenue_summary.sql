{{
  config(
    materialized='view',
    tags=['marts', 'revenue', 'daily']
  )
}}

-- Mart model: Daily aggregation of revenue, yield, and BIFI buyback metrics (last 30 days)
-- This model provides daily totals for:
-- - revenue_usd: Total revenue from BIFI buybacks and feebatch harvests (treasury amounts)
-- - yield_usd: Total yield from harvest events
-- - bifi_buyback_usd: Total BIFI buyback amount in USD
-- Limited to the last 30 days for performance
-- Built directly from staging models without relying on other marts

WITH bifi_buyback_daily AS (
  SELECT
    toDate(txn_timestamp) as date_day,
    -- Calculate and sum buyback_usd: bifi_amount * bifi_price
    sum(toDecimal256(bifi_amount * bifi_price, 20)) as bifi_revenue_usd,
    sum(toDecimal256(bifi_amount * bifi_price, 20)) as bifi_buyback_usd
  FROM {{ ref('stg_beefy_db_configs__bifi_buyback') }}
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    buyback_total IS NOT NULL
    AND txn_timestamp IS NOT NULL
    AND bifi_amount > 0
    AND bifi_price > 0
    AND toDate(txn_timestamp) >= today() - 30
  GROUP BY toDate(txn_timestamp)
),

feebatch_revenue_daily AS (
  SELECT
    toDate(txn_timestamp) as date_day,
    -- Sum treasury_amt: this is the revenue from feebatch harvests
    sum(toDecimal256(treasury_amt, 20)) as treasury_revenue_usd
  FROM {{ ref('stg_beefy_db_configs__feebatch_harvests') }}
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    treasury_amt IS NOT NULL
    AND txn_timestamp IS NOT NULL
    AND treasury_amt > 0
    AND toDate(txn_timestamp) >= today() - 30
  GROUP BY toDate(txn_timestamp)
),

yield_daily AS (
  SELECT
    toDate(date_time) as date_day,
    sum(underlying_amount_compounded_usd) as yield_usd
  FROM {{ ref('int_yield') }}
  WHERE toDate(date_time) >= today() - 30
  GROUP BY toDate(date_time)
)

SELECT
  coalesce(b.date_day, f.date_day, y.date_day) as date_day,
  coalesce(f.treasury_revenue_usd, 0) as revenue_usd,
  coalesce(y.yield_usd, 0) as yield_usd,
  coalesce(b.bifi_buyback_usd, 0) as bifi_buyback_usd
FROM bifi_buyback_daily b
FULL OUTER JOIN feebatch_revenue_daily f
  ON b.date_day = f.date_day
FULL OUTER JOIN yield_daily y
  ON coalesce(b.date_day, f.date_day) = y.date_day
WHERE coalesce(b.date_day, f.date_day, y.date_day) >= today() - 30
ORDER BY date_day DESC

