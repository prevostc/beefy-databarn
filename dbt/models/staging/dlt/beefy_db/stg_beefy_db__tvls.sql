{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(vault_id as String) as vault_id,
  cast(t as DateTime('UTC')) as t,
  {{ to_decimal('val') }} as val
FROM {{ source('dlt', 'beefy_db___tvls') }}

