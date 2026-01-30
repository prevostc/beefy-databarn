{{
  config(
    materialized='materialized_view',
    order_by=['date_time', 'vault_id'],
  )
}}

-- Staging as table + MV so int_product_stats__tvl_hourly_mv can fire on insert.
-- MV is triggered by INSERT into dlt.beefy_db___tvls (this model's source).
SELECT
  cast(t.vault_id as String) as vault_id,
  cast(t.t as DateTime('UTC')) as date_time,
  {{ to_float('t.val') }} as tvl_usd
FROM {{ source('dlt', 'beefy_db___tvls') }} t

