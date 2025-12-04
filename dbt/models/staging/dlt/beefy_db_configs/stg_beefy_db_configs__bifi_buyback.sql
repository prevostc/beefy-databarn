{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(id as String) as id,
  cast(block_number as Int64) as block_number,
  cast(txn_timestamp as DateTime('UTC')) as txn_timestamp,
  cast(event_idx as Int64) as event_idx,
  {{ evm_transaction_hash('txn_hash') }} as txn_hash,
  {{ to_decimal('bifi_amount') }} as bifi_amount,
  {{ to_decimal('bifi_price') }} as bifi_price,
  {{ to_decimal('buyback_total') }} as buyback_total
FROM {{ source('dlt', 'beefy_db_configs___bifi_buyback') }}

