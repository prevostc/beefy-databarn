{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  vault_id
FROM dlt.beefy_db_configs___vault_ids

