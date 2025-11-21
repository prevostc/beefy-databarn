{{
  config(
    materialized='view',
  )
}}

SELECT
  chain_id,
  address,
  is_contract,
  label
FROM dlt.beefy_db___address_metadata

