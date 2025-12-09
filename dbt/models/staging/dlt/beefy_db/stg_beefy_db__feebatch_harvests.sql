{{
  config(
    materialized='view',
  )
}}

SELECT
  cast(_dlt_id as String) as _dlt_id,
  cast(chain_id as String) as chain_id,
  cast(block_number as Int64) as block_number,
  cast(txn_timestamp as DateTime('UTC')) as txn_timestamp,
  {{ evm_transaction_hash('txn_hash') }} as txn_hash,
  {{ to_decimal('harvest_usd') }} as harvest_usd,
  {{ to_decimal('treasury_amt') }} as treasury_amt,
  {{ to_decimal('rewardpool_amt') }} as rewardpool_amt,
  {{ to_decimal('native_price') }} as native_price,
  {{ to_decimal('stable_price') }} as stable_price
FROM {{ source('dlt', 'beefy_db___feebatch_harvests') }}

