{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  t.chainId as network_id,
  {{ evm_address('t.address') }} as address,
  t.shareToken_id as share_token_id,
  t.underlyingToken_id as underlying_token_id,
  t.initializableStatus as initializable_status,
  t.initializedBlock as initialized_block,
  t.initializedTimestamp as initialized_timestamp
FROM {{ source('envio', 'ClassicVault') }} t

