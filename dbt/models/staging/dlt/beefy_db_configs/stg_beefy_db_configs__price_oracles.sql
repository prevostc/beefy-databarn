{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  oracle_id,
  tokens
FROM dlt.beefy_db_configs___price_oracles

