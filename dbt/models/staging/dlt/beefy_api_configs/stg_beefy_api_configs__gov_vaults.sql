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
  earned_token_addresses,
  earned_token_decimals,
  zaps,
  assumeNotNull(id) as id,
  name,
  type,
  version,
  token,
  token_address,
  token_decimals,
  token_provider_id,
  earn_contract_address,
  earned_token,
  oracle,
  oracle_id,
  status,
  created_at,
  platform_id,
  strategy_type_id,
  network,
  toBool(is_gov_vault) as is_gov_vault,
  chain,
  total_supply,
  last_harvest,
  earned_token_decimals__v_json,
  retired_at,
  retire_reason,
  earned_token_address,
  toBool(excluded) as excluded,
  buy_token_url,
  updated_at,
  toBool(earning_points) as earning_points
FROM {{ source('dlt', 'beefy_api_configs___gov_vaults') }}

