{{
  config(
    materialized='view',
  )
}}

SELECT
  chain_id,
  name,
  beefy_name,
  enabled
FROM dlt.beefy_db___chains

