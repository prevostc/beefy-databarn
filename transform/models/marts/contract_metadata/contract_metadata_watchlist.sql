{{
  config(
    materialized='view'
  )
}}

with

vaults as (

    select *
    from {{ ref('int_is_active_vaults' ) }}

),

boosts as (

    select *
    from {{ ref('int_is_active_boosts' ) }}

),

zaps as (

    select *
    from {{ ref('stg_zaps' ) }}

),

active_chains as (
    select chain
    from {{ ref('stg_chains') }}
    where is_active
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
        contract_address,
        is_active
    from vaults

    union all

    select
        chain,
        contract_address,
        is_active
    from boosts

    union all

    select
        chain,
        router_contract_address,
        true as is_active
    from zaps

),

contracts_to_import as (

    select *
    from all_contracts
    where
        chain in (select chain from active_chains)
        and (chain, contract_address) not in (
            select
                chain,
                contract_address
            from already_imported_contracts
        )

)

select
    chain,
    {{ bytea_to_hex_text("contract_address") }} as contract_address,
    is_active
from contracts_to_import
-- ingesting active contracts first
order by is_active::integer desc
