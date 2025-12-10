{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.vault_id as String) as vault_id,
  cast(t.t as DateTime('UTC')) as date_time,
  {{ to_float('t.val') }} as tvl
FROM {{ source('dlt', 'beefy_db___tvls') }} t FINAL

