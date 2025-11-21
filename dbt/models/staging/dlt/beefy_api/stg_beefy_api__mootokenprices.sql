{{
  config(
    materialized='view',
  )
}}

SELECT
  etag,
  chain_id,
  moo_token_symbol,
  price,
  date_time
FROM dlt.beefy_api___mootokenprices

