{{
  config(
    materialized='view',
    tags=['intermediate', 'yield']
  )
}}

-- Intermediate model: Transform harvest events into yield structure
-- This model maps harvest events to a yield model, where harvest_amount * want_price_usd represents yield
-- Handles data quality issues, standardizes formats, and prepares for yield aggregation

WITH cleaned_yield AS (
  SELECT
    -- Create composite key for unique identification
    {{ dbt_utils.generate_surrogate_key(['h.chain_id', 'h.block_number', 'h.txn_idx', 'h.event_idx']) }} as id,
    h.chain_id as dim_chain_id,
    dc.chain_name as dim_chain_name,
    dp.dim_product_id,
    h.block_number,
    -- Standardize timestamp (handle timezone issues if any)
    toDateTime(h.txn_timestamp) as dim_time,
    h.txn_idx,
    h.event_idx,
    lower(h.txn_hash) as txn_hash,
    h.harvest_amount,
    h.want_price as want_price_usd,
    -- Calculate yield: harvest_amount * want_price_usd
    -- Ensure proper Decimal multiplication with explicit casting
    -- Cast result to Decimal256(20) to maintain full precision
    toDecimal256(h.harvest_amount * h.want_price, 20) as yield_usd
  FROM {{ ref('stg_beefy_db__harvests') }} h
  INNER JOIN {{ ref('dim_chain') }} dc
    ON h.chain_id = dc.dim_chain_id
  INNER JOIN {{ ref('dim_product') }} dp
    ON h.vault_id = dp.beefy_id
  WHERE
    -- Filter out invalid records (ensure yield data quality)
    h.harvest_amount IS NOT NULL
    AND h.want_price IS NOT NULL
    AND h.txn_timestamp IS NOT NULL
    AND h.harvest_amount > 0
    AND h.want_price > 0
    -- price is off for some harvests in the feed
    -- biggest real harvest we saw was this ($9M)
    -- https://etherscan.io/tx/0x31b8083e467ed217523655f9b26b71f154fd1358e633b275011123c268a88901
    -- next biggest harvest is bugged and shows as $32M
    AND toDecimal256(h.harvest_amount * h.want_price, 20) < 30_000_000.00
)

-- Output: Clean yield events ready for aggregation
-- Each row represents a yield-generating harvest event

SELECT
  id,
  dim_time,
  dim_chain_id,
  dim_chain_name,
  dim_product_id,
  block_number,
  txn_idx,
  event_idx,
  txn_hash,
  harvest_amount,
  want_price_usd,
  yield_usd
FROM cleaned_yield

