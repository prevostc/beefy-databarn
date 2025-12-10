{{
  config(
    materialized='materialized_view',
    tags=['dimension', 'products'],
    order_by=['chain_id', 'product_address'],
  )
}}

-- Dimension table: Product (Vault) reference data
-- This table provides product/vault metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
    pc.chain_id,
    'classic' as product_type,
    pc.vault_address as product_address,
    pc.beefy_key,
    pc.display_name,
    pc.is_active,
    pc.platform_id
FROM {{ ref('product_classic') }} pc

UNION ALL

SELECT
    clm.chain_id,
    'clm' as product_type,
    clm.vault_address as product_address,
    clm.beefy_key,
    clm.display_name,
    clm.is_active,
    clm.platform_id
FROM {{ ref('product_clm') }} clm

UNION ALL

SELECT
    rp.chain_id,
    'reward_pool' as product_type,
    rp.reward_pool_address as product_address,
    rp.beefy_key,
    rp.display_name,
    rp.is_active,
    rp.platform_id
FROM {{ ref('product_reward_pool') }} rp

UNION ALL

SELECT
    cb.chain_id,
    'boost' as product_type,
    cb.boost_address as product_address,
    cb.beefy_key,
    cb.display_name,
    cb.is_active,
    null as platform_id
FROM {{ ref('product_classic_boost') }} cb
