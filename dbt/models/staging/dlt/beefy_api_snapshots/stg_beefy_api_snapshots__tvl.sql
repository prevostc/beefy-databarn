{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(etag) as etag,
  assumeNotNull(network_id) as network_id,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull(tvl) as tvl,
  assumeNotNull(date_time) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___tvl') }}

