{{
  config(
    materialized='table',
    tags=['data_quality', 'debug']
  )
}}

-- Data Quality Model: Products missing in envio
-- Identifies products from the product dimension that exist on chains tracked by envio
-- but are not present in any envio staging models
-- This helps debug data coverage gaps

with envio_chains as (
  SELECT DISTINCT chain_id FROM {{ ref('int_envio__chains') }}
),

missing_classic as (
  SELECT
    p.chain_id as chain_id,
    c.chain_name as chain_name,
    p.vault_address as product_address,
    p.beefy_key as beefy_key,
    p.display_name as display_name,
    p.is_active as is_active,
    'stg_envio__classic_vault' as missing_in_envio_model
  FROM {{ ref('product_classic') }} p
  INNER JOIN envio_chains ec
    ON p.chain_id = ec.chain_id
  INNER JOIN {{ ref('chain') }} c
    ON p.chain_id = c.chain_id
  WHERE (p.chain_id, p.vault_address) NOT IN (
      SELECT network_id, address
      FROM {{ ref('stg_envio__classic_vault') }}
    )
),

missing_clm as (
  SELECT
    p.chain_id as chain_id,
    c.chain_name as chain_name,
    p.vault_address as product_address,
    p.beefy_key as beefy_key,
    p.display_name as display_name,
    p.is_active as is_active,
    'stg_envio__clm_manager' as missing_in_envio_model
  FROM {{ ref('product_clm') }} p
  INNER JOIN envio_chains ec
    ON p.chain_id = ec.chain_id
  INNER JOIN {{ ref('chain') }} c
    ON p.chain_id = c.chain_id
  WHERE (p.chain_id, p.vault_address) NOT IN (
      SELECT network_id, address
      FROM {{ ref('stg_envio__clm_manager') }}
    )
),

missing_reward_pool as (
  SELECT
    p.chain_id as chain_id,
    c.chain_name as chain_name,
    p.reward_pool_address as product_address,
    p.beefy_key as beefy_key,
    p.display_name as display_name,
    p.is_active as is_active,
    'stg_envio__reward_pool' as missing_in_envio_model
  FROM {{ ref('product_reward_pool') }} p
  INNER JOIN envio_chains ec
    ON p.chain_id = ec.chain_id
  INNER JOIN {{ ref('chain') }} c
    ON p.chain_id = c.chain_id
  WHERE (p.chain_id, p.reward_pool_address) NOT IN (
      SELECT network_id, address
      FROM {{ ref('stg_envio__reward_pool') }}
    )
),

missing_classic_boost as (
  SELECT
    p.chain_id as chain_id,
    c.chain_name as chain_name,
    p.boost_address as product_address,
    p.beefy_key as beefy_key,
    p.display_name as display_name,
    p.is_active as is_active,
    'stg_envio__classic_boost' as missing_in_envio_model
  FROM {{ ref('product_classic_boost') }} p
  INNER JOIN envio_chains ec
    ON p.chain_id = ec.chain_id
  INNER JOIN {{ ref('chain') }} c
    ON p.chain_id = c.chain_id
  WHERE (p.chain_id, p.boost_address) NOT IN (
      SELECT network_id, address
      FROM {{ ref('stg_envio__classic_boost') }}
    )
),
all_missing as ( 
  SELECT * FROM missing_classic
  UNION ALL
  SELECT * FROM missing_clm
  UNION ALL
  SELECT * FROM missing_reward_pool
  UNION ALL
  SELECT * FROM missing_classic_boost
)

select * from all_missing