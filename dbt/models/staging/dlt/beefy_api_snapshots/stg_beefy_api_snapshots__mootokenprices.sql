{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(etag) as etag,
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(moo_token_symbol) as moo_token_symbol,
  assumeNotNull(price) as price,
  assumeNotNull(date_time) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___mootokenprices') }}
where price is not null
