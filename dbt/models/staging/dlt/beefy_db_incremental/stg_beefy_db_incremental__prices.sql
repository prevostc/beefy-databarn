{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(oracle_id) as oracle_id,
  assumeNotNull(t) as t,
  assumeNotNull({{ to_decimal('val') }}) as val
FROM {{ source('dlt', 'beefy_db_incremental___prices') }}

