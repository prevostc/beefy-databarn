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
  vaults.platform_id as platform_id,
  toBool(ifNull(vaults.status = 'active', false)) as is_active,
  vaults.strategy as latest_strategy_address,
  {{ to_representation_evm_address('vaults.token_address') }} as underlying_token_representation_address,
  envio.initialized_block as creation_block,
  envio.initialized_timestamp as creation_datetime
FROM {{ ref('stg_beefy_api__vaults') }} vaults
LEFT JOIN {{ ref('chain') }} chain_dim
  ON vaults.network = chain_dim.beefy_key
LEFT JOIN {{ ref('stg_envio__classic_vault') }} envio
  ON chain_dim.chain_id = envio.network_id
  AND vaults.earn_contract_address = envio.address
WHERE NOT vaults.is_gov_vault
