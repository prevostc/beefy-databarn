{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(id as String) as id,
  cast(oracle_id as String) as oracle_id,
  ifNull(tokens, '[]') as tokens
FROM {{ source('dlt', 'beefy_db___price_oracles') }}

