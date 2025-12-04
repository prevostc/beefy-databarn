{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(id as String) as id,
  cast(chain_id as String) as chain_id,
  cast(block_number as Int64) as block_number,
  {{ evm_transaction_hash('txn_hash') }} as txn_hash,
  cast(vault_id as String) as vault_id,
  {{ evm_address('vault_address') }} as vault_address,
  {{ evm_address('strategy_address') }} as strategy_address
FROM {{ source('dlt', 'beefy_db_configs___vault_strategies') }}

