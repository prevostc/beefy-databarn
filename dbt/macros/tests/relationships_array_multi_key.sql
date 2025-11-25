{% test relationships_array_multi_key(
     model,
     to,
     from_array_column,
     from_chain_column,
     to_chain_column,
     to_address_column
) %}

{%- set from_array_column_quoted = adapter.quote(from_array_column) if from_array_column else 'array_col' %}
{%- set from_chain_column_quoted = adapter.quote(from_chain_column) if from_chain_column else 'chain_id' %}
{%- set to_chain_column_quoted = adapter.quote(to_chain_column) if to_chain_column else 'chain_id' %}
{%- set to_address_column_quoted = adapter.quote(to_address_column) if to_address_column else 'address' %}

with array_elements as (
    select
        {{ from_chain_column_quoted }},
        arrayJoin({{ from_array_column_quoted }}) as token_address
    from {{ model }}
    where {{ from_array_column_quoted }} is not null
),
pk as (
    select
        {{ to_chain_column_quoted }},
        {{ to_address_column_quoted }}
    from {{ to }}
),
invalid as (
    select 
        array_elements.{{ from_chain_column_quoted }},
        array_elements.token_address
    from array_elements
    left join pk
        on array_elements.{{ from_chain_column_quoted }} = pk.{{ to_chain_column_quoted }}
        and array_elements.token_address = pk.{{ to_address_column_quoted }}
    where pk.{{ to_chain_column_quoted }} is null
)

select * from invalid

{% endtest %}

