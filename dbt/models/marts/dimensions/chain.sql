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
  chain_id as chain_id,
  chain_id as network_id,
  name as chain_name,
  {{ normalize_network_beefy_key('beefy_name') }} as beefy_key,
  enabled != 0 as beefy_enabled
FROM {{ ref('stg_beefy_db_configs__chains') }}

