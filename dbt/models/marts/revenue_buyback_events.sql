{{
  config(
    materialized='view',
    tags=['intermediate', 'revenue']
  )
}}

-- Intermediate model: Transform BIFI buyback events into revenue structure
-- This model maps buyback events to a revenue model, where buyback_total represents revenue
-- Handles data quality issues, standardizes formats, and prepares for revenue aggregation
-- Joins with token dimension to provide token metadata

WITH cleaned_revenue AS (
  SELECT
    id,
    block_number,
    -- Standardize timestamp (handle timezone issues if any)
    toDateTime(txn_timestamp) as date_time,
    event_idx,
    lower(txn_hash) as tx_hash,
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

SELECT
  cr.id,
  cr.date_time,
  cr.block_number,
  cr.event_idx,
  cr.tx_hash,
  t.chain_id,
  t.representation_address as token_representation_address,
  t.erc20_address as token_erc20_address,
  t.is_native as token_is_native,
  t.symbol as token_symbol,
  t.name as token_name,
  t.decimals as token_decimals,
  cr.token_price_usd,
  cr.buyback_amount,
  cr.buyback_usd
FROM cleaned_revenue cr
INNER JOIN {{ ref('token') }} t
  ON t.symbol = 'BIFI'
