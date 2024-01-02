{{
  config(
    materialized='view'
  )
}}

with

vaults as (

    select * from {{ ref('int_active_vaults' ) }}

),

boosts as (

    select * from {{ ref('int_active_boosts' ) }}

),

already_imported_contracts as (

    select
        chain,
        contract_address
    from {{ ref('stg_contract_creation_date') }}

),

all_contracts as (

    select
        chain,
        contract_address
    from vaults

    union all

    select
        chain,
        contract_address
    from boosts

),

contracts_to_import as (

    select
        chain,
        contract_address
    from all_contracts

    except

    select
        chain,
        contract_address
    from already_imported_contracts

)

select
  chain,
  {{ bytea_to_hex_text("contract_address") }} as contract_address
from contracts_to_import
