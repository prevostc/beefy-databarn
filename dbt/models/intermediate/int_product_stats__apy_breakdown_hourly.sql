{{
  config(
    materialized='materialized_view',
    tags=['intermediate', 'product_stats'],
    order_by=['date_hour', 'chain_id', 'product_address'],
  )
}}


SELECT
  p.chain_id,
  p.product_address,
  toStartOfHour(ab.date_time) as date_hour,
  argMax(ab.compoundings_per_year, ab.date_time) as compoundings_per_year,
  argMax(ab.beefy_performance_fee, ab.date_time) as beefy_performance_fee,
  argMax(ab.lp_fee, ab.date_time) as lp_fee,
  argMax(ab.total_apy, ab.date_time) as total_apy,
  argMax(ab.vault_apr, ab.date_time) as vault_apr,
  argMax(ab.trading_apr, ab.date_time) as trading_apr,
  argMax(ab.clm_apr, ab.date_time) as clm_apr,
  argMax(ab.reward_pool_apr, ab.date_time) as reward_pool_apr,
  argMax(ab.reward_pool_trading_apr, ab.date_time) as reward_pool_trading_apr,
  argMax(ab.vault_apy, ab.date_time) as vault_apy,
  argMax(ab.liquid_staking_apr, ab.date_time) as liquid_staking_apr,
  argMax(ab.composable_pool_apr, ab.date_time) as composable_pool_apr,
  argMax(ab.merkl_apr, ab.date_time) as merkl_apr,
  argMax(ab.linea_ignition_apr, ab.date_time) as linea_ignition_apr
FROM {{ ref('stg_beefy_api__apy_breakdown') }} ab
INNER JOIN {{ ref('product') }} p
  ON ab.vault_id = p.beefy_key
GROUP BY p.chain_id, p.product_address, toStartOfHour(ab.date_time)
