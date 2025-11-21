{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  chain_id,
  block_number,
  txn_hash,
  vault_id,
  vault_address,
  strategy_address
FROM dlt.beefy_db___vault_strategies

