{{
  config(
    materialized='view',
  )
}}

SELECT
  price,
  usd_value,
  balance,
  assumeNotNull(etag) as etag,
  assumeNotNull(mm_id) as mm_id,
  assumeNotNull(exchange_name) as exchange_name,
  assumeNotNull(token_symbol) as token_symbol,
  assumeNotNull(date_time) as date_time,
  symbol,
  name,
  oracle_id,
  oracle_type
FROM {{ source('dlt', 'beefy_api_snapshots___treasury_mm') }}

