{{
  config(
    materialized='view'
  )
}}

with base as (
    select *
    from {{ source('tap_beefy_api', 'beefy_vaults') }}
),

eol_chains as (
    select t.chain
    from (values ('heco'), ('celo'), ('emerald'), ('one')) as t (chain)
),

distinct_chains as (
    select distinct chain
    from base
)

select
    chain,
    chain not in (select chain from eol_chains) as is_active
from distinct_chains
