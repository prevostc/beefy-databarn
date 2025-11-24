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
FROM dlt.beefy_api_snapshots___apy
where apy is not null
