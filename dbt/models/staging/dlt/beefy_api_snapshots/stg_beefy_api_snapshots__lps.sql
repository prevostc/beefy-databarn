{{
  config(
    materialized='view',
  )
}}

SELECT
  etag,
  vault_id,
  lps,
  date_time
FROM dlt.beefy_api_snapshots___lps

