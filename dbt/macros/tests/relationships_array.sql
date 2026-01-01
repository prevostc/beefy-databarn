{% test relationships_array(
     model,
     column_name,
     to,
     from_array_column=None,
     to_column=None
) %}

{%- set from_array_column_quoted = adapter.quote(from_array_column if from_array_column else column_name) %}
{%- set to_column_quoted = adapter.quote(to_column if to_column else 'chain_id') %}

with array_elements as (
    select
        arrayJoin({{ from_array_column_quoted }}) as array_value
    from {{ model }}
    where {{ from_array_column_quoted }} is not null
      and length({{ from_array_column_quoted }}) > 0
),
pk as (
    select
        {{ to_column_quoted }}
    from {{ to }}
),
invalid as (
    select 
        array_elements.array_value
    from array_elements
    left join pk
        on array_elements.array_value = pk.{{ to_column_quoted }}
    where pk.{{ to_column_quoted }} is null
)

select * from invalid

{% endtest %}

