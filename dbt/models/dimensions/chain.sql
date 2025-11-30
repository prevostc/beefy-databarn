{{
  config(
    materialized='table',
    tags=['dimension', 'chains']
  )
}}

-- Dimension table: Chain reference data
-- This table provides chain metadata for joining with fact tables
-- Small reference table, materialized as table for performance

SELECT
  ui.chain_id,
  ui.chain_id as network_id,
  assumeNotNull(ui.name) as chain_name,
  {{ normalize_network_beefy_key('ui.chain_key') }} as beefy_key,
  assumeNotNull(toBool(case 
    when ui.eol_date_time is null then true
    when db.enabled is not null then db.enabled
    when ui.ui_disabled is not null then not ui.ui_disabled
    else false
  end)) as beefy_enabled,
  ui.rpc as public_rpc_urls,
  ui.explorer_url,
  assumeNotNull(ui.multicall3_address) as multicall3_address,
  assumeNotNull(ui.app_multicall_contract_address) as ui_lens_address,
  ui.gas_type,
  ui.eol_date_time
FROM {{ ref('stg_github_files___beefy_ui_chains') }} ui
 LEFT JOIN {{ ref('stg_beefy_db_configs__chains') }} db
  ON ui.chain_id = db.chain_id