{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(oracle_id as String) as oracle_id,
  cast(t as DateTime('UTC')) as t,
  {{ to_decimal('val') }} as val
FROM {{ source('dlt', 'beefy_db_incremental___prices') }}

