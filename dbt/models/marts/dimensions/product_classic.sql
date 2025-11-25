{{
  config(
    materialized='table',
    tags=['dimension', 'products'],
    engine='MergeTree()',
    order_by=['is_active', 'chain_id', 'vault_address'],
  )
}}

-- Dimension table: Product (Vault) reference data
-- This table provides product/vault metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  chain_dim.chain_id || ':' || {{ format_hex('vaults.earn_contract_address') }} as product_id,
  vaults.id as beefy_key,
  chain_dim.chain_id as chain_id,
  vaults.name as display_name,
  toBool(assumeNotNull(vaults.status = 'active')) as is_active,
  vaults.earn_contract_address as vault_address,
  vaults.strategy as latest_strategy_address,
  vaults.token_address as underlying_token_address
FROM {{ ref('stg_beefy_api_configs__vaults') }} vaults
LEFT JOIN {{ ref('chain') }} chain_dim
  ON vaults.network = chain_dim.beefy_key
WHERE NOT vaults.is_gov_vault
