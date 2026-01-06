{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}


SELECT
  prod.chain_id,
  prod.product_address as token_address,
  p.t as date_time,
  p.val as price
FROM {{ ref('stg_beefy_db__prices') }} p FINAL
INNER JOIN {{ ref('stg_beefy_db__price_oracles') }} o
  ON p.oracle_id = o.id
INNER JOIN {{ ref('product') }} prod
  ON o.oracle_id = prod.beefy_price_oracle_key
ORDER BY prod.chain_id, prod.product_address, p.t

