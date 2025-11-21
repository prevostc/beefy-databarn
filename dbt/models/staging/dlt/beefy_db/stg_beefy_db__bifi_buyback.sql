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
  txn_hash,
  bifi_amount,
  bifi_price,
  buyback_total
FROM dlt.beefy_db___bifi_buyback

