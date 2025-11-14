{{
  config(
    materialized='view',
    tags=['intermediate', 'yield']
  )
}}

-- Intermediate model: Transform harvest events into yield structure
-- This model maps harvest events to a yield model, where harvest_amount * want_price_usd represents yield
-- Handles data quality issues, standardizes formats, and prepares for yield aggregation

WITH cleaned_yield AS (
  SELECT
    -- Create composite key for unique identification
    {{ dbt_utils.generate_surrogate_key(['h.chain_id', 'h.block_number', 'h.txn_idx', 'h.event_idx']) }} as id,
    h.chain_id as dim_chain_id,
    dc.chain_name as dim_chain_name,
    dp.dim_product_id,
    h.block_number,
    -- Standardize timestamp (handle timezone issues if any)
    toDateTime(h.txn_timestamp) as dim_time,
    h.txn_idx,
    h.event_idx,
    lower(h.txn_hash) as txn_hash,
    h.harvest_amount,
    h.want_price as want_price_usd,
    -- Calculate yield: harvest_amount * want_price_usd
    -- Ensure proper Decimal multiplication with explicit casting
    -- Cast result to Decimal256(20) to maintain full precision
    toDecimal256(h.harvest_amount * h.want_price, 20) as yield_usd
  FROM {{ ref('stg_beefy_db__harvests') }} h
  INNER JOIN {{ ref('dim_chain') }} dc
    ON h.chain_id = dc.dim_chain_id
  INNER JOIN {{ ref('dim_product') }} dp
    ON h.vault_id = dp.beefy_id
  WHERE
    -- Filter out invalid records (ensure yield data quality)
    h.harvest_amount IS NOT NULL
    AND h.want_price IS NOT NULL
    AND h.txn_timestamp IS NOT NULL
    AND h.harvest_amount > 0
    AND h.want_price > 0
    -- price is off for some harvests in the feed
    -- biggest real harvest we saw was this ($9M)
    -- https://etherscan.io/tx/0x31b8083e467ed217523655f9b26b71f154fd1358e633b275011123c268a88901
    -- next biggest harvest is bugged and shows as $32M
    AND toDecimal256(h.harvest_amount * h.want_price, 20) < 30_000_000.00

    AND (chain_id, lower(txn_hash)) NOT IN (
      -- armads: "this one doesnt even have any large transfers in the tx 😅, same for the other avax transactions"
      (43114 /* avax */, '0xfd904cb8742ea0caa10bc8a1475f487a2af885938997ff00dcbc195533961162'),
      (43114 /* avax */, '0xffe824f13634da2f10daedb3c3cc123fea419e1b03fb6d169e9a98c89a29100e'),
      (43114 /* avax */, '0x058d86ee2e888dbbf3bcbd20adc71739e6d29feae0074a944dfaad6a892384c3'),
      (43114 /* avax */, '0x83a4b2356d4d18228d02755ddd13a5b6f932fa8a611620580b6ed8d04320c021'),
      -- armads: "optimism ones too. I was thinking maybe there was a flashloan in there but i dont see any"
      (10 /* optimism */, '0x079606a933abf8dce650c09da15a065afca30fa34e8c3aa714205d63015031a2'),
      (10 /* optimism */, '0x5d0538d3c5888cfa803329410df043a289f8cca32563c497e8b9ba835d28a3c7'),
      (10 /* optimism */, '0xf5db5dff133fee010a2ee689f1fd62a2fb9879d9e34533028daa641ab05a2c4b'),
      -- armads: "ah the 2 eth txs both have large flashloans"
      (1 /* ethereum */, '0xec308aaa41bea030e06de958af46179803f96c3c78950b8da2c4912a1775d87a'),
      (1 /* ethereum */, '0x31b8083e467ed217523655f9b26b71f154fd1358e633b275011123c268a88901'),

      -- this is obviously mispriced
      (1 /* ethereum */, '0x06163765e870e2adfbe047560c62162a9aab772007b313ea2c96827c4277d1d1'),
      (43114 /* avax */, '0x091c7e4427dedf0c35bf327f2a7d88541667b322036d8935a8e7c276d7a66091'),
      (8453 /* base */, '0x2d27615057903b479a5c2ba5c2c7a85633563bf36074ac918c6b8c4eaf3f4cc0'),
      (8453 /* base */, '0xb19cb06de752f37fb548e91f23b0e7fbd94a27bcbb2952c10c431a57694519de'),
    )
)

-- Output: Clean yield events ready for aggregation
-- Each row represents a yield-generating harvest event

SELECT
  id,
  dim_time,
  dim_chain_id,
  dim_chain_name,
  dim_product_id,
  block_number,
  txn_idx,
  event_idx,
  txn_hash,
  harvest_amount,
  want_price_usd,
  yield_usd
FROM cleaned_yield

