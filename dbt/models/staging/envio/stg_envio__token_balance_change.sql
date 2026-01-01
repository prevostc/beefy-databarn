{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  t.chainId as network_id,
  t.tokenBalance_id as token_balance_id,
  t.account_id as account_id,
  t.token_id as token_id,
  {{ evm_transaction_hash('t.trxHash') }} as trx_hash,
  t.blockNumber as block_number,
  t.blockTimestamp as block_timestamp,
  t.balanceBefore as balance_before,
  t.balanceAfter as balance_after
FROM {{ source('envio', 'TokenBalanceChange') }} t

