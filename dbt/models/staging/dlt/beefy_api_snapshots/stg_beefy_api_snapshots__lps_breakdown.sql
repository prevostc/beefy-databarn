{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(etag) as etag,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull(date_time) as date_time,
  price,
  tokens,
  balances,
  total_supply,
  underlying_liquidity,
  underlying_balances,
  underlying_price
FROM {{ source('dlt', 'beefy_api_snapshots___lps_breakdown') }}

