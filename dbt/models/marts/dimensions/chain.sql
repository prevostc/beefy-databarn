{{
  config(
    materialized='table',
    tags=['dimension', 'chains']
  )
}}

-- Dimension table: Chain reference data
-- This table provides chain metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  chain_id as dim_chain_id,
  chain_id as network_id,
  name as chain_name,
  beefy_name as beefy_key,
  enabled != 0 as beefy_enabled
FROM {{ ref('stg_beefy_db__chains') }}

