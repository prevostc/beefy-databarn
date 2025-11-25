{{
  config(
    materialized='table',
    tags=['dimension', 'tokens']
  )
}}

-- Dimension table: Token reference data
-- This table provides token metadata for joining with fact tables
-- Small reference table, materialized as table for performance


SELECT
  chain_dim.chain_id || ':' || {{ format_hex('tokens.address') }} as token_id,
  chain_dim.chain_id as chain_id,
  tokens.symbol,
  tokens.name,
  tokens.address,
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