{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(block_number) as block_number,
  assumeNotNull(txn_idx) as txn_idx,
  assumeNotNull(event_idx) as event_idx,
  assumeNotNull(txn_timestamp) as txn_timestamp,
  assumeNotNull({{ evm_transaction_hash('txn_hash') }}) as txn_hash,
  assumeNotNull(vault_id) as vault_id,
  {{ to_decimal('call_fee') }} as call_fee,
  {{ to_decimal('gas_fee') }} as gas_fee,
  {{ to_decimal('platform_fee') }} as platform_fee,
  {{ to_decimal('strategist_fee') }} as strategist_fee,
  assumeNotNull({{ to_decimal('harvest_amount') }}) as harvest_amount,
  {{ to_decimal('native_price') }} as native_price,
  {{ to_decimal('want_price') }} as want_price,
  toBool(is_cowllector) as is_cowllector,
  {{ evm_address('strategist_address') }} as strategist_address
FROM {{ source('dlt', 'beefy_db_incremental___harvests') }}

