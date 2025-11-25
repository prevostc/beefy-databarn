{{
  config(
    materialized='view',
  )
}}

SELECT
  assets,
  partners,
  period_finishes,
  assumeNotNull(id) as id,
  name,
  chain,
  pool_id,
  version,
  status,
  earn_contract_address,
  {{ evm_address('token_address') }} as underlying_token_address,
  earned_token,
  earned_token_decimals,
  {{ evm_address('earned_token_address') }} as boost_contract_address,
  earned_oracle,
  earned_oracle_id,
  partnership,
  toBool(is_moo_staked) as is_moo_staked,
  period_finish,
  campaign
FROM {{ source('dlt', 'beefy_api_configs___boosts') }}

