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
  JSONExtract(ifNull(deposit_token_addresses, '[]'), 'Array(String)') as deposit_token_addresses,
  zaps,
  vault,
  pool,
  cast(id as String) as id,
  ifNull(name, 'Unknown') as name,
  token,
  cast({{ evm_address('token_address') }} as String) as token_address,
  token_decimals,
  token_provider_id,
  earned_token,
  cast({{ evm_address('earned_token_address') }} as String) as earned_token_address,
  cast({{ evm_address('earn_contract_address') }} as String) as earn_contract_address,
  oracle,
  oracle_id,
  status,
  created_at,
  cast(platform_id as String) as platform_id,
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

