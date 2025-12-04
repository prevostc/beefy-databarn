{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(chain_id as String) as chain_id,
  {{ evm_address('address') }} as address,
  toBool(is_contract) as is_contract,
  label
FROM {{ source('dlt', 'beefy_db_configs___address_metadata') }}

