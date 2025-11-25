{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(etag) as etag,
  assumeNotNull(token_symbol) as token_symbol,
  assumeNotNull(price) as price,
  assumeNotNull(date_time) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___prices') }}

