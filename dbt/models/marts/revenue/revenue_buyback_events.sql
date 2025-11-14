{{
  config(
    materialized='view',
    tags=['intermediate', 'revenue']
  )
}}

-- Intermediate model: Transform BIFI buyback events into revenue structure
-- This model maps buyback events to a revenue model, where buyback_total represents revenue
-- Handles data quality issues, standardizes formats, and prepares for revenue aggregation

WITH cleaned_revenue AS (
  SELECT
    id,
    block_number,
    -- Standardize timestamp (handle timezone issues if any)
    toDateTime(txn_timestamp) as date_time,
    event_idx,
    lower(txn_hash) as tx_hash,
    -- Token symbol is BIFI (the token being bought back)
    'BIFI' AS token_symbol,
    bifi_price as token_price_usd,
    bifi_amount as buyback_amount,
    -- Ensure proper Decimal multiplication with explicit casting
    -- Multiply as Decimal128 to avoid precision/overflow issues
    bifi_amount * bifi_price as buyback_usd
  FROM {{ ref('stg_beefy_db__bifi_buyback') }}
  WHERE
    -- Filter out invalid records (ensure revenue data quality)
    buyback_total IS NOT NULL
    AND txn_timestamp IS NOT NULL
    AND bifi_amount > 0
    AND bifi_price > 0
)

-- Output: Clean revenue events ready for aggregation
-- Each row represents a revenue-generating buyback event

SELECT
  id,
  date_time,
  block_number,
  event_idx,
  tx_hash,
  token_symbol,
  token_price_usd,
  buyback_amount,
  buyback_usd
FROM cleaned_revenue
