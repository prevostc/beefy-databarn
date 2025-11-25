{{
  config(
    materialized='view',
  )
}}

SELECT
  type,
  assumeNotNull(id) as id,
  symbol,
  name,
  {{ normalize_network_beefy_key('chain_id') }} as chain_beefy_key,
  oracle,
  oracle_id,
  {{ evm_address('address') }} as address,
  toInt64(decimals) as decimals,
  bridge,
  toBool(staked) as staked
FROM {{ source('dlt', 'beefy_api_configs___tokens') }}

