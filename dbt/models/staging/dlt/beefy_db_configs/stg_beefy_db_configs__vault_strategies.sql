{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  chain_id,
  block_number,
  {{ hex_to_bytes('txn_hash') }} as txn_hash,
  {{ normalize_hex_string('txn_hash') }} as txn_hash_hex,
  vault_id,
  {{ hex_to_bytes('vault_address') }} as vault_address,
  {{ normalize_hex_string('vault_address') }} as vault_address_hex,
  {{ hex_to_bytes('strategy_address') }} as strategy_address,
  {{ normalize_hex_string('strategy_address') }} as strategy_address_hex
FROM dlt.beefy_db_configs___vault_strategies

