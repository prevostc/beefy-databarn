{{
  config(
    materialized='view',
  )
}}

SELECT
  apy,
  etag,
  vault_id,
  date_time,
  apy__v_text
FROM dlt.beefy_api___apy

