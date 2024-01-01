{{
  config(
    materialized='table'
  )
}}

with base as (
    select *
    from {{ source('tap_rest_api_msdk', 'beefy_vaults') }}
)
select distinct chain
from base
