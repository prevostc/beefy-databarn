{{
  config(
    materialized='view',
  )
}}

SELECT
  t.id as id,
  t.chainId as network_id,
  t.account_id as account_id,
  t.token_id as token_id,
  t.amount as amount
FROM {{ source('envio', 'TokenBalance') }} t

