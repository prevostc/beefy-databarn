{{
  config(
    materialized='view',
    tags=['intermediate', 'product_stats'],
   
  )
}}


-- Materialized intermediate: Hourly tvl aggregations
-- This reduces memory usage by materializing tvl aggregations separately
-- Combines tvl staging data with product information and aggregates to hourly

WITH tvl_with_product AS (
  SELECT
    p.chain_id,
    p.product_address,
    a.date_time,
    a.tvl
  FROM {{ ref('stg_beefy_db__tvls') }} a
  INNER JOIN {{ ref('stg_beefy_db__vault_ids') }} vi
    ON a.vault_id = vi.vault_id
  INNER JOIN {{ ref('product') }} p
    ON vi.beefy_key = p.beefy_key
)

SELECT
  chain_id,
  product_address,
  toStartOfHour(date_time) as date_hour,
  argMax(tvl, date_time) as tvl
FROM tvl_with_product
GROUP BY chain_id, product_address, toStartOfHour(date_time)
