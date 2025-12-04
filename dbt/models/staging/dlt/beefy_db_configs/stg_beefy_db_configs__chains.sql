{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(chain_id as Int64) as network_id,
  ifNull(name, 'Unknown') as name,
  ifNull(beefy_name, 'Unknown') as beefy_name,
  toBool(enabled != 0) as enabled
FROM {{ source('dlt', 'beefy_db_configs___chains') }}

