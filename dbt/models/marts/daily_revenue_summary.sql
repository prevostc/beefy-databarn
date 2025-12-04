{{
  config(
    materialized='table',
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
    0 as yield_usd,
    0 as revenue_usd,
    sum(toDecimal256(bifi_amount * bifi_price, 20)) as bifi_buyback_usd
  FROM {{ ref('stg_beefy_db_configs__bifi_buyback') }}
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    buyback_total IS NOT NULL
    AND txn_timestamp IS NOT NULL
    AND bifi_amount > 0
    AND bifi_price > 0
    -- Filter out invalid timestamps that would convert to 1970-01-01
    AND toDate(txn_timestamp) > '1970-01-01'
  GROUP BY toDate(txn_timestamp)
),

feebatch_revenue_daily AS (
  SELECT
    toDate(txn_timestamp) as date_day,
    0 as yield_usd,
    sum(toDecimal256(treasury_amt, 20)) as revenue_usd,
    0 as bifi_buyback_usd
  FROM {{ ref('stg_beefy_db_configs__feebatch_harvests') }}
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    treasury_amt IS NOT NULL
    AND txn_timestamp IS NOT NULL
    AND treasury_amt > 0
    -- Filter out invalid timestamps that would convert to 1970-01-01
    AND toDate(txn_timestamp) > '1970-01-01'
  GROUP BY toDate(txn_timestamp)
),

yield_daily AS (
  SELECT
    toDate(date_time) as date_day,
    sum(underlying_amount_compounded_usd) as yield_usd,
    0 as revenue_usd,
    0 as bifi_buyback_usd
  FROM {{ ref('int_yield') }}
  WHERE
    -- Filter out invalid timestamps that would convert to 1970-01-01
    date_time IS NOT NULL
    AND toDate(date_time) > '1970-01-01'
  GROUP BY toDate(date_time)
),

all_rows as (
  select * from yield_daily
  union all
  select * from feebatch_revenue_daily
  union all
  select * from bifi_buyback_daily
)

SELECT
  date_day as date_day,
  coalesce(sum(yield_usd), 0) as yield_usd,
  coalesce(sum(revenue_usd), 0) as revenue_usd,
  coalesce(sum(bifi_buyback_usd), 0) as bifi_buyback_usd
FROM all_rows
GROUP BY date_day
ORDER BY date_day DESC

