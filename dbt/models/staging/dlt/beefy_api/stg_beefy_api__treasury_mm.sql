{{
  config(
    materialized='view',
  )
}}

SELECT
  price,
  usd_value,
  balance,
  cast(etag as String) as etag,
  cast(mm_id as String) as mm_id,
  exchange_name as exchange_name,
  token_symbol as token_symbol,
  cast(date_time as DateTime('UTC')) as date_time,
  symbol,
  name,
  oracle_id,
  oracle_type
FROM {{ source('dlt', 'beefy_api___treasury_mm') }}

