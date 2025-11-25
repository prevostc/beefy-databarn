{{
  config(
    materialized='table',
    tags=['dimension', 'products']
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
    is_active
FROM {{ ref('product_classic') }}

UNION ALL

SELECT
    chain_id,
    'clm' as product_type,
    vault_address as product_address,
    beefy_key,
    display_name,
    is_active
FROM {{ ref('product_clm') }}

