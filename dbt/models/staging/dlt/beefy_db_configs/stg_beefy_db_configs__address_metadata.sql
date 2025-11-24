{{
  config(
    materialized='view',
  )
}}

SELECT
  chain_id,
  {{ hex_to_bytes('address') }} as address,
  {{ normalize_hex_string('address') }} as address_hex,
  is_contract,
  label
FROM dlt.beefy_db_configs___address_metadata

