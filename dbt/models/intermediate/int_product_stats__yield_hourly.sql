{{
  config(
    materialized='materialized_view',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
    on_schema_change='append_new_columns',
  )
}}


-- Materialized intermediate: Hourly tvl aggregations
-- This reduces memory usage by materializing tvl aggregations separately
-- Combines tvl staging data with product information and aggregates to hourly

WITH yield_with_product AS (
  SELECT
    a.date_time,
    a.chain_id,
    a.product_address,
    a.underlying_amount_compounded,
    a.underlying_token_price_usd,
    a.underlying_amount_compounded_usd
  FROM {{ ref('int_yield') }} a
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(underlying_amount_compounded, date_time) as underlying_amount_compounded,
  argMax(underlying_token_price_usd, date_time) as underlying_token_price_usd,
  argMax(underlying_amount_compounded_usd, date_time) as underlying_amount_compounded_usd
FROM yield_with_product
GROUP BY chain_id, product_address, toStartOfHour(date_time)
