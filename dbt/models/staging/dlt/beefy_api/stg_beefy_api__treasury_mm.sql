{{
  config(
    materialized='view',
  )
}}

SELECT
  t.price,
  t.usd_value,
  t.balance,
  cast(t.etag as String) as etag,
  cast(t.mm_id as String) as mm_id,
  t.exchange_name as exchange_name,
  t.token_symbol as token_symbol,
  cast(t.date_time as DateTime('UTC')) as date_time,
  t.symbol,
  t.name,
  t.oracle_id,
  t.oracle_type
FROM {{ source('dlt', 'beefy_api___treasury_mm') }} t

