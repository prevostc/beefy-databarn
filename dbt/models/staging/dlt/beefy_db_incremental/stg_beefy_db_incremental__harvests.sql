{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(chain_id as Int64) as network_id,
  cast(block_number as Int64) as block_number,
  cast(txn_idx as Int32) as txn_idx,
  cast(event_idx as Int32) as event_idx,
  cast(txn_timestamp as DateTime('UTC')) as txn_timestamp,
  cast(lower({{ evm_transaction_hash('txn_hash') }}) as String) as txn_hash,
  cast(vault_id as String) as vault_id,
  toDecimal256(ifNull({{ to_decimal('call_fee') }}, 0), 20) as call_fee,
  toDecimal256(ifNull({{ to_decimal('gas_fee') }}, 0), 20) as gas_fee,
  toDecimal256(ifNull({{ to_decimal('platform_fee') }}, 0), 20) as platform_fee,
  toDecimal256(ifNull({{ to_decimal('strategist_fee') }}, 0), 20) as strategist_fee,
  toDecimal256(ifNull({{ to_decimal('harvest_amount') }}, 0), 20) as harvest_amount,
  toDecimal256(ifNull({{ to_decimal('native_price') }}, 0), 20) as native_price,
  toDecimal256(ifNull({{ to_decimal('want_price') }}, 0), 20) as want_price,
  toBool(ifNull(is_cowllector, false)) as is_cowllector,
  cast({{ evm_address('strategist_address') }} as String) as strategist_address
FROM {{ source('dlt', 'beefy_db_incremental___harvests') }}

