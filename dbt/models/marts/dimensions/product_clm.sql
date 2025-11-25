{{
  config(
    materialized='table',
    tags=['dimension', 'products'],
    engine='MergeTree()',
    order_by=['chain_id', 'vault_address'],
  )
}}

-- Dimension table: Product (Vault) reference data
-- This table provides product/vault metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  chain_dim.chain_id as chain_id,
  vaults.earn_contract_address as vault_address,
  vaults.id as beefy_key,
  vaults.name as display_name,
  toBool(assumeNotNull(vaults.status = 'active')) as is_active,
  vaults.strategy as latest_strategy_address,
  vaults.token_address as lp_token_address,
  {{ evm_address('vaults.deposit_token_addresses[1]') }} as underlying_token_0_address,
  {{ evm_address('vaults.deposit_token_addresses[2]') }} as underlying_token_1_address
FROM {{ ref('stg_beefy_api_configs__clm_vaults') }} vaults
LEFT JOIN {{ ref('chain') }} chain_dim
  ON vaults.network = chain_dim.beefy_key
WHERE NOT vaults.is_gov_vault
