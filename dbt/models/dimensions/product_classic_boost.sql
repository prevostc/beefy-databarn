{{
  config(
    materialized='table',
    tags=['dimension', 'products'],
    engine='MergeTree()',
    order_by=['chain_id', 'boost_address'],
  )
}}


select 
  chain_dim.chain_id as chain_id,
  boost.boost_contract_address as boost_address,
  boost.id as beefy_key,
  boost.id as display_name,
  toBool(ifNull(boost.status = 'active', false)) as is_active,
  {{ to_representation_evm_address('boost.underlying_token_address') }} as underlying_token_representation_address,
  {{ to_representation_evm_address('boost.reward_token_address') }} as reward_token_representation_address
from {{ ref('stg_beefy_api_configs__boosts') }} boost
join {{ ref('chain') }} chain_dim on boost.chain = chain_dim.beefy_key
where boost.version is null or boost.version < 2