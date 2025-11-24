{{
  config(
    materialized='view',
  )
}}

SELECT
  etag,
  network_id,
  vault_id,
  tvl,
  date_time
FROM dlt.beefy_api_snapshots___tvl

