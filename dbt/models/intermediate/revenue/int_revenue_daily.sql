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
    toDateTime(txn_timestamp) AS event_timestamp,
    event_idx,
    txn_hash,
    bifi_amount,
    bifi_price,
    -- Revenue: buyback_total represents the revenue generated from the buyback
    buyback_total AS revenue_amount,
    -- Currency is BIFI (the token being bought back)
    'BIFI' AS currency,
    -- Extract date for grouping
    toDate(txn_timestamp) AS event_date
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
  block_number,
  event_timestamp,
  event_idx,
  txn_hash,
  bifi_amount,
  bifi_price,
  revenue_amount,
  currency,
  event_date
FROM cleaned_revenue

