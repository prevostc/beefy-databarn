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
        contract_address,
        chain,
        '{"IERC20_Transfer"}'::text [] as events
    from vaults
    where is_active

    union all

    select
        contract_address,
        chain,
        '{"IERC20_Transfer"}'::text [] as events
    from boosts
    where is_active

    union all

    select
        router_contract_address as contract_address,
        chain,
        '{"BeefyZapRouter_FulfilledOrder"}'::text [] as events
    from zaps
)

select
    c.chain,
    {{ bytea_to_hex_text("c", "contract_address") }} as contract_address,
    c.events,
    cb.block_number as creation_block_number,
    cb.block_datetime as creation_block_datetime
from alls_contracts c
join contract_creation_blocks cb using (contract_address, chain)
