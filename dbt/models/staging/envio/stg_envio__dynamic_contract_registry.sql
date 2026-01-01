{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  t.chain_id as network_id,
  t.registering_event_block_number as registering_event_block_number,
  t.registering_event_log_index as registering_event_log_index,
  t.registering_event_block_timestamp as registering_event_block_timestamp,
  t.registering_event_contract_name as registering_event_contract_name,
  t.registering_event_name as registering_event_name,
  {{ evm_address('t.registering_event_src_address') }} as registering_event_src_address,
  {{ evm_address('t.contract_address') }} as contract_address,
  t.contract_name as contract_name
FROM {{ source('envio', 'dynamic_contract_registry') }} t

