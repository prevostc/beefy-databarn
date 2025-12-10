{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(t.chain_id as String) as chain_id,
  {{ evm_address('t.address') }} as address,
  toBool(t.is_contract) as is_contract,
  t.label
FROM {{ source('dlt', 'beefy_db___address_metadata') }} t

