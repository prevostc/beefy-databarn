{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(id) as id,
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(block_number) as block_number,
  {{ evm_transaction_hash('txn_hash') }} as txn_hash,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull({{ evm_address('vault_address') }}) as vault_address,
  {{ evm_address('strategy_address') }} as strategy_address
FROM {{ source('dlt', 'beefy_db_configs___vault_strategies') }}

