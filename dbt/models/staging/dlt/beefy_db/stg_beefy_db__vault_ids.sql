{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(id as String) as id,
  cast(vault_id as String) as vault_id
FROM {{ source('dlt', 'beefy_db___vault_ids') }}

