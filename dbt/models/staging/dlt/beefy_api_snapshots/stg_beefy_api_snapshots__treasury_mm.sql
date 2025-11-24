{{
  config(
    materialized='view',
  )
}}

SELECT
  price,
  usd_value,
  balance,
  etag,
  mm_id,
  exchange_name,
  token_symbol,
  date_time,
  symbol,
  name,
  oracle_id,
  oracle_type
FROM dlt.beefy_api_snapshots___treasury_mm

