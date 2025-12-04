{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(etag as String) as etag,
  cast(chain_id as String) as chain_id,
  ifNull(moo_token_symbol, 'Unknown') as moo_token_symbol,
  toFloat64(price) as price,
  cast(date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___mootokenprices') }}
where price is not null
