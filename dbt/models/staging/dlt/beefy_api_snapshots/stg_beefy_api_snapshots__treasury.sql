{{
  config(
    materialized='view',
  )
}}

SELECT
  token_decimals,
  token_price,
  usd_value,
  assumeNotNull(etag) as etag,
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(wallet_address) as wallet_address,
  wallet_name,
  assumeNotNull(token_address) as token_address,
  assumeNotNull(date_time) as date_time,
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

