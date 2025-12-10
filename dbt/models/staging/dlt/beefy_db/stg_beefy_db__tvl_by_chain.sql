{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as Int64) as network_id,
  cast(t.t as DateTime('UTC')) as date_time,
  {{ to_decimal('t.total') }} as tvl_usd,
  {{ to_decimal('t.vault') }} as vault_tvl_usd,
  {{ to_decimal('t.gov') }} as gov_tvl_usd,
  {{ to_decimal('t.clm') }} as clm_tvl_usd
FROM {{ source('dlt', 'beefy_db___tvl_by_chain') }} t FINAL

