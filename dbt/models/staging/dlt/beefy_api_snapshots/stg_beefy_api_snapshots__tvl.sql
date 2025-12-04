{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(etag as String) as etag,
  cast(network_id as Int64) as network_id,
  cast(vault_id as String) as vault_id,
  toFloat64(tvl) as tvl,
  cast(date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___tvl') }}

