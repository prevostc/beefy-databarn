{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  cast(t.chainId as Int64) as network_id,
  {{ evm_address('t.address') }} as address,
  t.clmManager_id as clm_manager_id,
  t.initializableStatus as initializable_status,
  t.initializedBlock as initialized_block,
  t.initializedTimestamp as initialized_timestamp
FROM {{ source('envio', 'ClmStrategy') }} t

