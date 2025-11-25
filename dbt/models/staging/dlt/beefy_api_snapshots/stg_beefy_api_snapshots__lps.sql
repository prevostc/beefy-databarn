{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(etag) as etag,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull(lps) as lps,
  assumeNotNull(date_time) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___lps') }}

