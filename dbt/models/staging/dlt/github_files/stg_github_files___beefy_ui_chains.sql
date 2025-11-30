{{
  config(
    materialized='view',
  )
}}

SELECT
  assumeNotNull(chain_id) as chain_id,
  assumeNotNull(name) as name,
  assumeNotNull(chain_key) as chain_key,
  {{ to_str_list('rpc') }} as rpc,
  explorer_url,
  assumeNotNull({{ evm_address('multicall3_address') }}) as multicall3_address,
  assumeNotNull({{ evm_address('app_multicall_contract_address') }}) as app_multicall_contract_address,
  assumeNotNull(JSONExtractString(native, 'symbol')) as native_token_symbol,
  assumeNotNull(JSONExtractString(native, 'oracleId')) as native_token_oracle_id,
  assumeNotNull(toInt64(JSONExtractString(native, 'decimals'))) as native_token_decimals,
  JSONExtractString(gas, 'type') as gas_type,
  case when eol is null then null else toDateTime(toInt64(eol)) end as eol_date_time,
  assumeNotNull(toBool(disabled)) as ui_disabled,
  assumeNotNull(toBool(new)) as ui_new
FROM {{ source('dlt', 'github_files___beefy_ui_chains') }}

