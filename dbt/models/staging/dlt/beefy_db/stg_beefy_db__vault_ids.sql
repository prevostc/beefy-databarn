{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  vault_id
FROM dlt.beefy_db___vault_ids

