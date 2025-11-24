{{
  config(
    materialized='view',
  )
}}

SELECT
  etag,
  token_symbol,
  price,
  date_time
FROM dlt.beefy_api_snapshots___prices

