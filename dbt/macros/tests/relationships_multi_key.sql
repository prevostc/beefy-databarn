{% test relationships_multi_key(
     model,
     to,
     from_columns,
     to_columns
) %}

with fk as (
    select
        {{ from_columns | join(', ') }}
    from {{ model }}
),
pk as (
    select
        {{ to_columns | join(', ') }}
    from {{ to }}
),
invalid as (
    select fk.*
    from fk
    left join pk
        on {% for i in range(from_columns|length) %}
            fk.{{ from_columns[i] }} = pk.{{ to_columns[i] }}
            {% if not loop.last %} and {% endif %}
        {% endfor %}
    where pk.{{ to_columns[0] }} is null
)

select * from invalid

{% endtest %}
