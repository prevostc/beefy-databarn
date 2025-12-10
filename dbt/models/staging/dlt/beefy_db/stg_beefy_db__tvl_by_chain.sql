{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.t as DateTime('UTC')) as t,
  {{ to_decimal('t.total') }} as total,
  {{ to_decimal('t.vault') }} as vault,
  {{ to_decimal('t.gov') }} as gov,
  {{ to_decimal('t.clm') }} as clm
FROM {{ source('dlt', 'beefy_db___tvl_by_chain') }} t FINAL

