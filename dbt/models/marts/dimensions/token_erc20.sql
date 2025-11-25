{{
  config(
    materialized='table',
    tags=['dimension', 'tokens'],
    order_by=['chain_id', 'address'],
  )
}}

-- Dimension table: Token reference data
-- This table provides token metadata for joining with fact tables
-- Small reference table, materialized as table for performance


SELECT
  chain_dim.chain_id as chain_id,
  assumeNotNull(tokens.address) as address,

  tokens.symbol,
  tokens.name,
  tokens.decimals,
  -- oracle,
  -- oracle_id,
  -- type,
  -- bridge,
  -- staked
FROM {{ ref('stg_beefy_api_configs__tokens') }} tokens
LEFT JOIN {{ ref('chain') }} chain_dim
  ON tokens.chain_beefy_key = chain_dim.beefy_key
WHERE tokens.address is not null