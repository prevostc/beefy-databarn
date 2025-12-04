{{
  config(
    materialized='view',
  )
}}

SELECT
  assets,
  partners,
  period_finishes,
  cast(id as String) as id,
  name,
  chain,
  pool_id,
  version,
  status,
  cast({{ evm_address('earn_contract_address') }} as String) as boost_contract_address,
  {{ evm_address('token_address') }} as underlying_token_address,
  earned_token,
  earned_token_decimals,
  cast({{ evm_address('earned_token_address') }} as String) as reward_token_address,
  earned_oracle,
  earned_oracle_id,
  partnership,
  toBool(is_moo_staked) as is_moo_staked,
  period_finish,
  campaign
FROM {{ source('dlt', 'beefy_api_configs___boosts') }}

