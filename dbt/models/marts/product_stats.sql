{{
  config(
    materialized='table',
    tags=['marts', 'tvl', 'stats'],
    engine='CoalescingMergeTree',
    order_by=['date_hour', 'chain_id', 'product_address'],
    post_hook=["OPTIMIZE TABLE {{ this }} FINAL"],
  )
}}

SELECT
  hs.date_hour,
  hs.chain_id,
  hs.product_address,
  p.product_type,
  p.beefy_key,
  p.display_name,
  p.is_active,
  p.platform_id,
  hs.tvl,
  hs.apy,
  hs.compoundings_per_year,
  hs.beefy_performance_fee,
  hs.lp_fee,
  hs.total_apy,
  hs.vault_apr,
  hs.trading_apr,
  hs.clm_apr,
  hs.reward_pool_apr,
  hs.reward_pool_trading_apr,
  hs.vault_apy,
  hs.liquid_staking_apr,
  hs.composable_pool_apr,
  hs.merkl_apr,
  hs.linea_ignition_apr,
  hs.lp_price,
  hs.breakdown_tokens,
  hs.breakdown_balances,
  hs.total_supply,
  hs.underlying_liquidity,
  hs.underlying_balances,
  hs.underlying_price,
FROM {{ ref('int_product_stats__unified_hourly') }} hs
INNER JOIN {{ ref('product') }} p
  ON hs.chain_id = p.chain_id
  AND hs.product_address = p.product_address