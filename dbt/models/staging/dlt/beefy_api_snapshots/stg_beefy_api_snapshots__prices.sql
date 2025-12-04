{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(etag as String) as etag,
  token_symbol as token_symbol,
  toFloat64(price) as price,
  cast(date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___prices') }}

