{{
  config(
    materialized='view',
  )
}}

SELECT
  assets,
  partners,
  period_finishes,
  id,
  name,
  chain,
  pool_id,
  version,
  status,
  earn_contract_address,
  token_address,
  earned_token,
  earned_token_decimals,
  earned_token_address,
  earned_oracle,
  earned_oracle_id,
  partnership,
  is_moo_staked,
  period_finish,
  campaign
FROM dlt.beefy_api_configs___boosts

