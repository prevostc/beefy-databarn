{{
  config(
    materialized='view',
  )
}}

SELECT
  oracle_id,
  t,
  val
FROM dlt.beefy_db___prices

