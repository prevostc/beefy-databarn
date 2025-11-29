{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(_dlt_id) as _dlt_id,
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(block_number) as block_number,
  assumeNotNull(txn_timestamp) as txn_timestamp,
  assumeNotNull({{ evm_transaction_hash('txn_hash') }}) as txn_hash,
  {{ to_decimal('harvest_usd') }} as harvest_usd,
  {{ to_decimal('treasury_amt') }} as treasury_amt,
  {{ to_decimal('rewardpool_amt') }} as rewardpool_amt,
  {{ to_decimal('native_price') }} as native_price,
  {{ to_decimal('stable_price') }} as stable_price
FROM {{ source('dlt', 'beefy_db_configs___feebatch_harvests') }}

