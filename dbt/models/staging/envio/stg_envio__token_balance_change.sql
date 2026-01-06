{{
  config(
    materialized='view',
  )
}}

-- Staging model: Token balance changes with extracted chain_id and contract_address
-- The id field format is: <chain_id>-<contract_address>
-- This avoids the need to join with stg_envio__token to get the token address
-- contract_address equals product_address for share tokens

with token_balance_change as (
  select 
    t.*, 
    splitByChar('-', t.token_id)[2] as token_contract_address 
  from {{ source('envio', 'TokenBalanceChange') }} t 
)
SELECT
  t.id as id,
  toInt64(t.chainId) as chain_id,
  toInt64(t.chainId) as network_id,
  t.tokenBalance_id as token_balance_id,
  t.account_id as account_id,
  t.token_id as token_id,
  {{ evm_transaction_hash('t.trxHash') }} as trx_hash,
  t.logIndex as log_index,
  toInt64(t.blockNumber) as block_number,
  t.blockTimestamp as block_timestamp,
  {{ to_decimal('t.balanceBefore') }} as balance_before,
  {{ to_decimal('t.balanceAfter') }} as balance_after,
  {{ to_representation_evm_address('t.token_contract_address') }} as token_contract_address,
  {{ to_decimal('t.balanceAfter') }} - {{ to_decimal('t.balanceBefore') }} as share_diff
FROM token_balance_change t


