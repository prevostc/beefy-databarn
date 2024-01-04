{{
  config(
    materialized='view'
  )
}}

with vaults as (
    select *
    from {{ ref('stg_vaults') }}
),

active_chains as (
    select chain
    from {{ ref('stg_chains') }}
    where is_active
)

select
    *,
    not eol
    and chain in (select chain from active_chains) as is_active
from vaults
