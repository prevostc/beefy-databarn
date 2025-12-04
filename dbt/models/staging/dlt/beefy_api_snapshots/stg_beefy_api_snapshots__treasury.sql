{{
  config(
    materialized='view',
  )
}}

SELECT
  token_decimals,
  token_price,
  usd_value,
  cast(etag as String) as etag,
  cast(chain_id as String) as chain_id,
  cast(wallet_address as String) as wallet_address,
  wallet_name,
  {{ evm_address('token_address') }} as token_address,
  cast(date_time as DateTime('UTC')) as date_time,
  name,
  address,
  decimals,
  asset_type,
  oracle_id,
  oracle_type,
  symbol,
  price,
  balance,
  id,
  toBool(staked) as staked,
  number_id,
  method,
  method_path,
  vault_id,
  price_per_full_share,
  helper
FROM {{ source('dlt', 'beefy_api_snapshots___treasury') }}

