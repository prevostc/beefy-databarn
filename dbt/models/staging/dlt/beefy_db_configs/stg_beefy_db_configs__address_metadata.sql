{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull({{ evm_address('address') }}) as address,
  toBool(is_contract) as is_contract,
  label
FROM {{ source('dlt', 'beefy_db_configs___address_metadata') }}

