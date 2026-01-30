{{
  config(
    materialized='materialized_view',
    order_by=['date_time', 'vault_id'],
  )
}}

-- Staging as table + MV so int_product_stats__apy_hourly_mv can fire on insert (trigger: dlt.beefy_db___apys).
SELECT
  cast(t.vault_id as String) as vault_id,
  cast(t.t as DateTime('UTC')) as date_time,
  {{ to_float('t.val') }} as apy,
  t.t as raw_date_time -- for fast filtering on incremental loads
FROM {{ source('dlt', 'beefy_db___apys') }} t 
 
