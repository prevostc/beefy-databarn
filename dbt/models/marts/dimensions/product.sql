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
  DISTINCT
  -- TODO: use a better id?
  vault_id as product_id,
  vault_id as beefy_id,
  vault_id as name
FROM {{ ref('stg_beefy_db__vault_ids') }}

