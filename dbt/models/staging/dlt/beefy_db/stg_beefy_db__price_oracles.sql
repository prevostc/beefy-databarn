{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  oracle_id,
  tokens
FROM dlt.beefy_db___price_oracles

