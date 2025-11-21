{{
  config(
    materialized='view',
  )
}}

SELECT
  token_decimals,
  token_price,
  usd_value,
  etag,
  chain_id,
  wallet_address,
  wallet_name,
  token_address,
  date_time,
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
  staked,
  number_id,
  method,
  method_path,
  vault_id,
  price_per_full_share,
  helper
FROM dlt.beefy_api___treasury

