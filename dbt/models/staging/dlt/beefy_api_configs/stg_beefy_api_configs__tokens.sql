{{
  config(
    materialized='view',
  )
}}

SELECT
  type,
  id,
  symbol,
  name,
  chain_id,
  oracle,
  oracle_id,
  address,
  decimals,
  bridge,
  staked
FROM dlt.beefy_api_configs___tokens

