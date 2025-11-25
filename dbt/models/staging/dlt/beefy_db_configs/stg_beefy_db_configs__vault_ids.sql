{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(id) as id,
  assumeNotNull(vault_id) as vault_id
FROM {{ source('dlt', 'beefy_db_configs___vault_ids') }}

