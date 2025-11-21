{{
  config(
    materialized='view',
  )
}}

SELECT
  etag,
  vault_id,
  date_time,
  price,
  tokens,
  balances,
  total_supply,
  underlying_liquidity,
  underlying_balances,
  underlying_price
FROM dlt.beefy_api___lps_breakdown

