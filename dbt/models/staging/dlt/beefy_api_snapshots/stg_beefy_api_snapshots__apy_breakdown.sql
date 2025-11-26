{{
  config(
    materialized='view',
  )
}}

SELECT
  toInt64(compoundings_per_year) as compoundings_per_year,
  toFloat64(beefy_performance_fee) as beefy_performance_fee,
  toFloat64(lp_fee) as lp_fee,
  toFloat64(total_apy) as total_apy,
  toFloat64(vault_apr) as vault_apr,
  toFloat64(trading_apr) as trading_apr,
  toFloat64(clm_apr) as clm_apr,
  toFloat64(reward_pool_apr) as reward_pool_apr,
  toFloat64(reward_pool_trading_apr) as reward_pool_trading_apr,
  assumeNotNull(etag) as etag,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull(date_time) as date_time,
  toFloat64(vault_apy) as vault_apy,
  toFloat64(liquid_staking_apr) as liquid_staking_apr,
  toFloat64(composable_pool_apr) as composable_pool_apr,
  toFloat64(merkl_apr) as merkl_apr,
  toFloat64(linea_ignition_apr) as linea_ignition_apr
FROM {{ source('dlt', 'beefy_api_snapshots___apy_breakdown') }}

