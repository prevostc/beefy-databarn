{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(id) as id,
  assumeNotNull(oracle_id) as oracle_id,
  assumeNotNull(tokens) as tokens
FROM {{ source('dlt', 'beefy_db_configs___price_oracles') }}

