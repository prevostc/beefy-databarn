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
FROM dlt.beefy_api_snapshots___mootokenprices
where price is not null
