{{
  config(
    materialized='view',
  )
}}

SELECT
  assets,
  risks,
  point_structure_ids,
  earned_oracle_ids,
  earned_tokens,
  JSONExtract(coalesce(earned_token_addresses, '[]'), 'Array(String)') as earned_token_addresses,
  earned_token_decimals,
  zaps,
  assumeNotNull(id) as id,
  assumeNotNull(name) as name,
  type,
  toInt32(version) as version,
  token,
  assumeNotNull({{ evm_address('token_address') }}) as token_address,
  assumeNotNull(toInt32(token_decimals)) as token_decimals,
  token_provider_id,
  assumeNotNull({{ evm_address('earn_contract_address') }}) as earn_contract_address,
  earned_token,
  oracle,
  oracle_id,
  status,
  created_at,
  assumeNotNull(platform_id) as platform_id,
  strategy_type_id,
  {{ normalize_network_beefy_key('network') }} as network,
  assumeNotNull(toBool(is_gov_vault)) as is_gov_vault,
  {{ normalize_network_beefy_key('chain') }} as chain,
  total_supply,
  last_harvest,
  earned_token_decimals__v_json,
  retired_at,
  retire_reason,
  assumeNotNull({{ evm_address('earned_token_address') }}) as earned_token_address,
  excluded as excluded,
  buy_token_url,
  updated_at,
  earning_points as earning_points
FROM {{ source('dlt', 'beefy_api_configs___gov_vaults') }}

