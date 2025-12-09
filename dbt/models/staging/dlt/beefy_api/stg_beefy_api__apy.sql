{{
  config(
    materialized='view',
  )
}}

SELECT
  toFloat64(apy) as apy,
  cast(etag as String) as etag,
  cast(vault_id as String) as vault_id,
  cast(date_time as DateTime('UTC')) as date_time,
  apy__v_text
FROM {{ source('dlt', 'beefy_api___apy') }}
where apy is not null
