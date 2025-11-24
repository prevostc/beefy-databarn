{{
  config(
    materialized='view',
  )
}}

SELECT
  chain_id,
  block_number,
  txn_idx,
  event_idx,
  txn_timestamp,
  {{ hex_to_bytes('txn_hash') }} as txn_hash,
  {{ normalize_hex_string('txn_hash') }} as txn_hash_hex,
  vault_id,
  {{ to_decimal('call_fee') }} as call_fee,
  {{ to_decimal('gas_fee') }} as gas_fee,
  {{ to_decimal('platform_fee') }} as platform_fee,
  {{ to_decimal('strategist_fee') }} as strategist_fee,
  {{ to_decimal('harvest_amount') }} as harvest_amount,
  {{ to_decimal('native_price') }} as native_price,
  {{ to_decimal('want_price') }} as want_price,
  is_cowllector,
  {{ hex_to_bytes('strategist_address') }} as strategist_address,
  {{ normalize_hex_string('strategist_address') }} as strategist_address_hex
FROM dlt.beefy_db_incremental___harvests

