{{
  config(
    materialized='view',
    tags=['intermediate', 'product_stats'],
  )
}}


-- Materialized intermediate: Unified hourly stats from all sources
-- Uses UNION ALL + aggregation instead of FULL OUTER JOINs for better memory efficiency
-- This avoids the memory-intensive join operations

WITH unified_data AS (
  -- TVL data
  SELECT
    chain_id,
    product_address,
    date_hour,
    tvl,
    NULL as apy,
    NULL as compoundings_per_year,
    NULL as beefy_performance_fee,
    NULL as lp_fee,
    NULL as total_apy,
    NULL as vault_apr,
    NULL as trading_apr,
    NULL as clm_apr,
    NULL as reward_pool_apr,
    NULL as reward_pool_trading_apr,
    NULL as vault_apy,
    NULL as liquid_staking_apr,
    NULL as composable_pool_apr,
    NULL as merkl_apr,
    NULL as linea_ignition_apr,
    NULL as lp_price,
    [] as breakdown_tokens,
    [] as breakdown_balances,
    NULL as total_supply,
    NULL as underlying_liquidity,
    [] as underlying_balances,
    NULL as underlying_price
  FROM {{ ref('int_product_stats__tvl_hourly') }}
  {% if is_incremental() %}
    WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
  {% endif %}

  UNION ALL

  -- APY data
  SELECT
    chain_id,
    product_address,
    date_hour,
    NULL as tvl,
    apy,
    NULL as compoundings_per_year,
    NULL as beefy_performance_fee,
    NULL as lp_fee,
    NULL as total_apy,
    NULL as vault_apr,
    NULL as trading_apr,
    NULL as clm_apr,
    NULL as reward_pool_apr,
    NULL as reward_pool_trading_apr,
    NULL as vault_apy,
    NULL as liquid_staking_apr,
    NULL as composable_pool_apr,
    NULL as merkl_apr,
    NULL as linea_ignition_apr,
    NULL as lp_price,
    [] as breakdown_tokens,
    [] as breakdown_balances,
    NULL as total_supply,
    NULL as underlying_liquidity,
    [] as underlying_balances,
    NULL as underlying_price
  FROM {{ ref('int_product_stats__apy_hourly') }}
  {% if is_incremental() %}
    WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
  {% endif %}

  UNION ALL

  -- APY breakdown data
  SELECT
    chain_id,
    product_address,
    date_hour,
    NULL as tvl,
    NULL as apy,
    compoundings_per_year,
    beefy_performance_fee,
    lp_fee,
    total_apy,
    vault_apr,
    trading_apr,
    clm_apr,
    reward_pool_apr,
    reward_pool_trading_apr,
    vault_apy,
    liquid_staking_apr,
    composable_pool_apr,
    merkl_apr,
    linea_ignition_apr,
    NULL as lp_price,
    [] as breakdown_tokens,
    [] as breakdown_balances,
    NULL as total_supply,
    NULL as underlying_liquidity,
    [] as underlying_balances,
    NULL as underlying_price
  FROM {{ ref('int_product_stats__apy_breakdown_hourly') }}
  {% if is_incremental() %}
    WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
  {% endif %}

  UNION ALL

  -- LPS breakdown data
  SELECT
    chain_id,
    product_address,
    date_hour,
    NULL as tvl,
    NULL as apy,
    NULL as compoundings_per_year,
    NULL as beefy_performance_fee,
    NULL as lp_fee,
    NULL as total_apy,
    NULL as vault_apr,
    NULL as trading_apr,
    NULL as clm_apr,
    NULL as reward_pool_apr,
    NULL as reward_pool_trading_apr,
    NULL as vault_apy,
    NULL as liquid_staking_apr,
    NULL as composable_pool_apr,
    NULL as merkl_apr,
    NULL as linea_ignition_apr,
    lp_price,
    breakdown_tokens,
    breakdown_balances,
    total_supply,
    underlying_liquidity,
    underlying_balances,
    underlying_price
  FROM {{ ref('int_product_stats__lps_breakdown_hourly') }}
  {% if is_incremental() %}
    WHERE date_hour >= toDateTime('{{ max_date }}') - INTERVAL 15 DAY
  {% endif %}
)

-- Aggregate to merge non-NULL values per hour
-- Since data is already hourly, we just need to pick non-NULL values
-- Using max() for scalars and any() for arrays (picks first non-NULL)
SELECT
  chain_id,
  product_address,
  date_hour,
  max(tvl) as tvl,
  max(apy) as apy,
  max(compoundings_per_year) as compoundings_per_year,
  max(beefy_performance_fee) as beefy_performance_fee,
  max(lp_fee) as lp_fee,
  max(total_apy) as total_apy,
  max(vault_apr) as vault_apr,
  max(trading_apr) as trading_apr,
  max(clm_apr) as clm_apr,
  max(reward_pool_apr) as reward_pool_apr,
  max(reward_pool_trading_apr) as reward_pool_trading_apr,
  max(vault_apy) as vault_apy,
  max(liquid_staking_apr) as liquid_staking_apr,
  max(composable_pool_apr) as composable_pool_apr,
  max(merkl_apr) as merkl_apr,
  max(linea_ignition_apr) as linea_ignition_apr,
  max(lp_price) as lp_price,
  max(breakdown_tokens) as breakdown_tokens,
  max(breakdown_balances) as breakdown_balances,
  max(total_supply) as total_supply,
  max(underlying_liquidity) as underlying_liquidity,
  max(underlying_balances) as underlying_balances,
  max(underlying_price) as price
FROM unified_data
GROUP BY chain_id, product_address, date_hour
