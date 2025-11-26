{{
  config(
    materialized='view',
    tags=['marts', 'tvl', 'stats']
  )
}}

-- Mart model: Timeseries of product stats (TVL, APY, breakdowns, price)
-- This model provides a complete timeseries of all product metrics by joining:
-- - TVL snapshots
-- - APY data
-- - APY breakdown (merged)
-- - LPS breakdown (merged)
-- - Price data (from LPS breakdown underlying_price)

WITH tvl_data AS (
  SELECT
    p.chain_id,
    p.product_address,
    t.date_time,
    t.tvl,
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
  FROM {{ ref('stg_beefy_api_snapshots__tvl') }} t
  INNER JOIN {{ ref('product') }} p
    ON t.network_id = p.chain_id
    AND t.vault_id = p.beefy_key
),

apy_data AS (
  SELECT
    p.chain_id,
    p.product_address,
    a.date_time,
    NULL as tvl,
    a.apy,
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
  FROM {{ ref('stg_beefy_api_snapshots__apy') }} a
  INNER JOIN {{ ref('product') }} p
    ON a.vault_id = p.beefy_key
),

apy_breakdown_data AS (
  SELECT
    p.chain_id,
    p.product_address,
    ab.date_time,
    NULL as tvl,
    NULL as apy,
    ab.compoundings_per_year,
    ab.beefy_performance_fee,
    ab.lp_fee,
    ab.total_apy,
    ab.vault_apr,
    ab.trading_apr,
    ab.clm_apr,
    ab.reward_pool_apr,
    ab.reward_pool_trading_apr,
    ab.vault_apy,
    ab.liquid_staking_apr,
    ab.composable_pool_apr,
    ab.merkl_apr,
    ab.linea_ignition_apr,
    NULL as lp_price,
    [] as breakdown_tokens,
    [] as breakdown_balances,
    NULL as total_supply,
    NULL as underlying_liquidity,
    [] as underlying_balances,
    NULL as underlying_price
  FROM {{ ref('stg_beefy_api_snapshots__apy_breakdown') }} ab
  INNER JOIN {{ ref('product') }} p
    ON ab.vault_id = p.beefy_key
),

lps_breakdown_data AS (
  SELECT
    p.chain_id,
    p.product_address,
    lb.date_time,
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
    lb.price as lp_price,
    lb.tokens as breakdown_tokens,
    lb.balances as breakdown_balances,
    lb.total_supply,
    lb.underlying_liquidity,
    lb.underlying_balances,
    lb.underlying_price
  FROM {{ ref('stg_beefy_api_snapshots__lps_breakdown') }} lb
  INNER JOIN {{ ref('product') }} p
    ON lb.vault_id = p.beefy_key
),

unified_data AS (
  SELECT * FROM tvl_data
  UNION ALL
  SELECT * FROM apy_data
  UNION ALL
  SELECT * FROM apy_breakdown_data
  UNION ALL
  SELECT * FROM lps_breakdown_data
),

hourly_stats AS (
  SELECT
    chain_id,
    product_address,
    toStartOfHour(date_time) as date_hour,
    argMax(tvl, date_time) as tvl,
    argMax(apy, date_time) as apy,
    -- APY breakdown fields
    argMax(compoundings_per_year, date_time) as compoundings_per_year,
    argMax(beefy_performance_fee, date_time) as beefy_performance_fee,
    argMax(lp_fee, date_time) as lp_fee,
    argMax(total_apy, date_time) as total_apy,
    argMax(vault_apr, date_time) as vault_apr,
    argMax(trading_apr, date_time) as trading_apr,
    argMax(clm_apr, date_time) as clm_apr,
    argMax(reward_pool_apr, date_time) as reward_pool_apr,
    argMax(reward_pool_trading_apr, date_time) as reward_pool_trading_apr,
    argMax(vault_apy, date_time) as vault_apy,
    argMax(liquid_staking_apr, date_time) as liquid_staking_apr,
    argMax(composable_pool_apr, date_time) as composable_pool_apr,
    argMax(merkl_apr, date_time) as merkl_apr,
    argMax(linea_ignition_apr, date_time) as linea_ignition_apr,
    -- LPS breakdown fields
    argMax(lp_price, date_time) as lp_price,
    argMax(breakdown_tokens, date_time) as breakdown_tokens,
    argMax(breakdown_balances, date_time) as breakdown_balances,
    argMax(total_supply, date_time) as total_supply,
    argMax(underlying_liquidity, date_time) as underlying_liquidity,
    argMax(underlying_balances, date_time) as underlying_balances,
    argMax(underlying_price, date_time) as price
  FROM unified_data
  GROUP BY chain_id, product_address, toStartOfHour(date_time)
)

SELECT
  p.product_type,
  p.beefy_key,
  p.display_name,
  p.is_active,
  p.platform_id,
  hs.*
FROM hourly_stats hs
INNER JOIN {{ ref('product') }} p
  ON hs.chain_id = p.chain_id
  AND hs.product_address = p.product_address

