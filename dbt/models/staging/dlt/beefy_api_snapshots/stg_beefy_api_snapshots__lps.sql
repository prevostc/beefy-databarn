{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(etag as String) as etag,
  cast(vault_id as String) as vault_id,
  toFloat64(lps) as lps,
  cast(date_time as DateTime('UTC')) as date_time
FROM {{ source('dlt', 'beefy_api_snapshots___lps') }}

