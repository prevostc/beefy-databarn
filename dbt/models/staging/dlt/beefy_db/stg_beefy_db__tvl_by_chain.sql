{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(chain_id as Int64) as network_id,
  cast(t as DateTime('UTC')) as t,
  {{ to_decimal('total') }} as total,
  {{ to_decimal('vault') }} as vault,
  {{ to_decimal('gov') }} as gov,
  {{ to_decimal('clm') }} as clm
FROM {{ source('dlt', 'beefy_db___tvl_by_chain') }} FINAL

