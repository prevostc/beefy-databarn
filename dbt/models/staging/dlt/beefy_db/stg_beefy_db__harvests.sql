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
  txn_hash,
  vault_id,
  call_fee,
  gas_fee,
  platform_fee,
  strategist_fee,
  harvest_amount,
  native_price,
  want_price,
  is_cowllector,
  strategist_address
FROM dlt.beefy_db___harvests

