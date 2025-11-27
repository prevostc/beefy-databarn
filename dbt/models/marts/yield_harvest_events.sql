{{
  config(
    materialized='view',
    tags=['intermediate', 'yield']
  )
}}

-- Mart model: Transform harvest events into yield structure with dimension joins
-- This model enriches the cleaned yield data from int_yield with chain and product dimensions
-- Uses the int_yield intermediate model which contains all yield business logic

SELECT
  cy.date_time,
  cy.chain_id as chain_id,
  dc.chain_name,
  dp.product_address,
  cy.block_number,
  cy.txn_idx as tx_idx,
  cy.event_idx,
  cy.tx_hash,
  cy.underlying_amount_compounded,
  cy.underlying_token_price_usd,
  cy.underlying_amount_compounded_usd
FROM {{ ref('int_yield') }} cy
INNER JOIN {{ ref('chain') }} dc
  ON cy.chain_id = dc.chain_id
INNER JOIN {{ ref('product') }} dp
  ON cy.chain_id = dp.chain_id
  AND cy.vault_id = dp.beefy_key

