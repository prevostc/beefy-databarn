{{
  config(
    materialized='view',
  )
}}

SELECT
  oracle_id,
  t,
  {{ to_decimal('val') }} as val
FROM dlt.beefy_db_incremental___prices

