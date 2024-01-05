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

)

select
    contract_address,
    chain,
    '{"IERC20:Transfer"}'::text [] as events
from vaults
where is_active

union all

select
    contract_address,
    chain,
    '{"IERC20:Transfer"}'::text [] as events
from boosts
where is_active

union all

select
    router_contract_address as contract_address,
    chain,
    '{"BeefyZapRouter:FulfilledOrder"}'::text [] as events
from zaps