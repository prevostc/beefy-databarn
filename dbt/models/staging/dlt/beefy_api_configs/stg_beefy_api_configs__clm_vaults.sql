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
  deposit_token_addresses,
  zaps,
  vault,
  pool,
  assumeNotNull(id) as id,
  name,
  token,
  token_address,
  token_decimals,
  token_provider_id,
  earned_token,
  earned_token_address,
  earn_contract_address,
  oracle,
  oracle_id,
  status,
  created_at,
  platform_id,
  strategy_type_id,
  network,
  type,
  toBool(is_gov_vault) as is_gov_vault,
  chain,
  strategy,
  last_harvest,
  retire_reason,
  retired_at,
  toBool(earning_points) as earning_points,
  updated_at
FROM {{ source('dlt', 'beefy_api_configs___clm_vaults') }}

