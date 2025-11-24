{{
  config(
    materialized='view',
  )
}}

SELECT
  compoundings_per_year,
  beefy_performance_fee,
  lp_fee,
  total_apy,
  vault_apr,
  trading_apr,
  clm_apr,
  reward_pool_apr,
  reward_pool_trading_apr,
  etag,
  vault_id,
  date_time,
  total_apy__v_text,
  vault_apy,
  liquid_staking_apr,
  composable_pool_apr,
  vault_apr__v_text,
  merkl_apr,
  linea_ignition_apr
FROM dlt.beefy_api_snapshots___apy_breakdown

