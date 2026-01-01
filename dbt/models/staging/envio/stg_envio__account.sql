{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  {{ evm_address('t.address') }} as address
FROM {{ source('envio', 'Account') }} t

