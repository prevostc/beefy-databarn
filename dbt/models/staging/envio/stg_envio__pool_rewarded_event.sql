{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  t.chainId as network_id,
  {{ evm_transaction_hash('t.trxHash') }} as trx_hash,
  t.logIndex as log_index,
  t.blockNumber as block_number,
  t.blockTimestamp as block_timestamp,
  t.poolShareToken_id as pool_share_token_id,
  t.rewardToken_id as reward_token_id,
  t.rewardAmount as reward_amount,
  t.rewardVestingSeconds as reward_vesting_seconds
FROM {{ source('envio', 'PoolRewardedEvent') }} t

