{{
  config(
    materialized='view',
  )
}}

SELECT
  assets,
  fee_tier,
  risks,
  point_structure_ids,
  JSONExtract(assumeNotNull(deposit_token_addresses), 'Array(String)') as deposit_token_addresses,
  zaps,
  vault,
  pool,
  assumeNotNull(id) as id,
  assumeNotNull(name) as name,
  token,
  assumeNotNull({{ evm_address('token_address') }}) as token_address,
  token_decimals,
  token_provider_id,
  earned_token,
  assumeNotNull({{ evm_address('earned_token_address') }}) as earned_token_address,
  assumeNotNull({{ evm_address('earn_contract_address') }}) as earn_contract_address,
  oracle,
  oracle_id,
  status,
  created_at,
  platform_id,
  strategy_type_id,
  {{ normalize_network_beefy_key('network') }} as network,
  type,
  toBool(is_gov_vault) as is_gov_vault,
  chain,
  {{ evm_address('strategy') }} as strategy,
  last_harvest,
  retire_reason,
  retired_at,
  toBool(earning_points) as earning_points,
  updated_at
FROM {{ source('dlt', 'beefy_api_configs___clm_vaults') }}

