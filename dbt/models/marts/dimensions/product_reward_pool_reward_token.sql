{{
  config(
    materialized='table',
    tags=['dimension', 'products', 'tokens'],
    engine='MergeTree()',
    order_by=['chain_id', 'reward_pool_address', 'token_representation_address'],
  )
}}

-- Join table: Product Reward Pool to Token ERC20
-- This table represents the relationship between reward pools and their earned tokens
-- Expands earned_token_addresses array from gov_vaults and joins with token

WITH reward_pool_expanded_tokens AS (
  SELECT
    vaults.earn_contract_address as reward_pool_address,
    vaults.network,
    assumeNotNull({{ evm_address('earned_token_addr') }}) as token_representation_address
  FROM {{ ref('stg_beefy_api_configs__gov_vaults') }} vaults
  ARRAY JOIN JSONExtract(assumeNotNull(earned_token_addresses), 'Array(String)') AS earned_token_addr
  WHERE vaults.version = 2
    AND earned_token_addresses IS NOT NULL
    AND earned_token_addr IS NOT NULL
    AND earned_token_addr != ''
)
SELECT
  chain_dim.chain_id as chain_id,
  et.reward_pool_address as reward_pool_address,
  t.representation_address as token_representation_address
FROM reward_pool_expanded_tokens et
LEFT JOIN {{ ref('chain') }} chain_dim
  ON et.network = chain_dim.beefy_key
INNER JOIN {{ ref('token') }} t
  ON chain_dim.chain_id = t.chain_id
  AND et.token_representation_address = t.representation_address
WHERE et.token_representation_address IS NOT NULL

