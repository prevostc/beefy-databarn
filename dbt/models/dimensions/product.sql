{{
  config(
    materialized='table',
    tags=['dimension', 'products'],
    engine='MergeTree()',
    order_by=['chain_id', 'product_address'],
  )
}}

-- Dimension table: Product (Vault) reference data
-- This table provides product/vault metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
    chain_id,
    'classic' as product_type,
    vault_address as product_address,
    beefy_key,
    display_name,
    is_active,
    platform_id
FROM {{ ref('product_classic') }}

UNION ALL

SELECT
    chain_id,
    'clm' as product_type,
    vault_address as product_address,
    beefy_key,
    display_name,
    is_active,
    platform_id
FROM {{ ref('product_clm') }}

UNION ALL

SELECT
    chain_id,
    'reward_pool' as product_type,
    reward_pool_address as product_address,
    beefy_key,
    display_name,
    is_active,
    platform_id
FROM {{ ref('product_reward_pool') }}

UNION ALL

SELECT
    chain_id,
    'boost' as product_type,
    boost_address as product_address,
    beefy_key,
    display_name,
    is_active,
    null as platform_id
FROM {{ ref('product_classic_boost') }}
