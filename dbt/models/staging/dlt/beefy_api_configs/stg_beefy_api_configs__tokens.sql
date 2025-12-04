{{
  config(
    materialized='view',
  )
}}

SELECT
  type,
  cast(id as String) as id,
  ifNull(symbol, 'Unknown') as symbol,
  ifNull(name, 'Unknown') as name,
  {{ normalize_network_beefy_key('chain_id') }} as chain_beefy_key,
  oracle,
  oracle_id,
  cast({{ evm_address('address') }} as String) as address,
  cast(decimals as Int64) as decimals,
  bridge,
  toBool(staked) as staked
FROM {{ source('dlt', 'beefy_api_configs___tokens') }}
where chain_id is not null
