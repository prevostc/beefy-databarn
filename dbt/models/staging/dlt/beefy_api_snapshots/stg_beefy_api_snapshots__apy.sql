{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(apy) as apy,
  assumeNotNull(etag) as etag,
  assumeNotNull(vault_id) as vault_id,
  assumeNotNull(date_time) as date_time,
  apy__v_text
FROM {{ source('dlt', 'beefy_api_snapshots___apy') }}
where apy is not null
