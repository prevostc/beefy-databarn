{{
  config(
    materialized='view',
  )
}}

SELECT
  t.assets,
  t.fee_tier,
  t.risks,
  t.point_structure_ids,
  JSONExtract(ifNull(t.deposit_token_addresses, '[]'), 'Array(String)') as deposit_token_addresses,
  t.zaps,
  t.vault,
  t.pool,
  cast(t.id as String) as id,
  ifNull(t.name, 'Unknown') as name,
  t.token,
  cast({{ evm_address('t.token_address') }} as String) as token_address,
  t.token_decimals,
  t.token_provider_id,
  t.earned_token,
  cast({{ evm_address('t.earned_token_address') }} as String) as earned_token_address,
  cast({{ evm_address('t.earn_contract_address') }} as String) as earn_contract_address,
  t.oracle,
  t.oracle_id,
  t.status,
  t.created_at,
  cast(t.platform_id as String) as platform_id,
  t.strategy_type_id,
  {{ normalize_network_beefy_key('t.network') }} as network,
  t.type,
  toBool(t.is_gov_vault) as is_gov_vault,
  t.chain,
  {{ evm_address('t.strategy') }} as strategy,
  t.last_harvest,
  t.retire_reason,
  t.retired_at,
  toBool(t.earning_points) as earning_points,
  t.updated_at
FROM {{ source('dlt', 'beefy_api___clm_vaults') }} t

