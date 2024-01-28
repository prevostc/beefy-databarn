{{
  config(
    materialized='view'
  )
}}

with

vaults as (

    select * from {{ ref('int_is_active_vaults' ) }}

),

boosts as (

    select * from {{ ref('int_is_active_boosts' ) }}

),

zaps as (

    select * from {{ ref('stg_zaps' ) }}

),

contract_creation_blocks as (
    select * from {{ ref('stg_contract_creation_date' ) }}
),

alls_contracts as (
    select
        chain,
        contract_address,
        '{"IERC20_Transfer"}'::text [] as events,
        is_active
    from vaults

    union all

    select
        chain,
        strategy_address as contract_address,
        '{"BeefyVault_UpgradeStrat"}'::text [] as events,
        is_active
    from vaults

    union all

    select
        chain,
        contract_address,
        '{"IERC20_Transfer"}'::text [] as events,
        is_active
    from boosts

    union all

    select
        chain,
        router_contract_address as contract_address,
        '{"BeefyZapRouter_FulfilledOrder"}'::text [] as events,
        true as is_active
    from zaps
)

select
    c.chain,
    {{ bytea_to_hex_text("c", "contract_address") }} as contract_address,
    c.events,
    cb.block_number as creation_block_number,
    cb.block_datetime as creation_block_datetime,
    c.is_active
from alls_contracts c
join contract_creation_blocks cb using (contract_address, chain)
