{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(name) as name,
  assumeNotNull(beefy_name) as beefy_name,
  toBool(enabled != 0) as enabled
FROM {{ source('dlt', 'beefy_db_configs___chains') }}

