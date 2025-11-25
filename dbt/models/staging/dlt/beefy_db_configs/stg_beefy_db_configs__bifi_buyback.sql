{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(id) as id,
  assumeNotNull(block_number) as block_number,
  assumeNotNull(txn_timestamp) as txn_timestamp,
  assumeNotNull(event_idx) as event_idx,
  assumeNotNull({{ evm_transaction_hash('txn_hash') }}) as txn_hash,
  assumeNotNull({{ to_decimal('bifi_amount') }}) as bifi_amount,
  assumeNotNull({{ to_decimal('bifi_price') }}) as bifi_price,
  assumeNotNull({{ to_decimal('buyback_total') }}) as buyback_total
FROM {{ source('dlt', 'beefy_db_configs___bifi_buyback') }}

