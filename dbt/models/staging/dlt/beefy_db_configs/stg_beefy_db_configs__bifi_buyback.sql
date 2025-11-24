{{
  config(
    materialized='view',
  )
}}

SELECT
  id,
  block_number,
  txn_timestamp,
  event_idx,
  {{ hex_to_bytes('txn_hash') }} as txn_hash,
  {{ normalize_hex_string('txn_hash') }} as txn_hash_hex,
  {{ to_decimal('bifi_amount') }} as bifi_amount,
  {{ to_decimal('bifi_price') }} as bifi_price,
  {{ to_decimal('buyback_total') }} as buyback_total
FROM dlt.beefy_db_configs___bifi_buyback

