{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  t.chainId as network_id,
  t.isVirtual as is_virtual,
  {{ evm_address('t.address') }} as address,
  t.symbol as symbol,
  t.name as name,
  t.decimals as decimals,
  t.totalSupply as total_supply,
  t.holderCount as holder_count
FROM {{ source('envio', 'Token') }} t

