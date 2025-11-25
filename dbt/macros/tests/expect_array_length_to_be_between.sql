{% test expect_array_length_to_be_between(model, column_name,
                                          min_value=None,
                                          max_value=None,
                                          row_condition=None
                                         ) %}

{%- set column_name_quoted = adapter.quote(column_name) if column_name else 'array_col' %}

with validation_errors as (
    select
        {{ column_name_quoted }}
    from {{ model }}
    where 1=1
    {% if row_condition %}
        and {{ row_condition }}
    {% endif %}
    and (
        {% if min_value is not none and max_value is not none %}
            length({{ column_name_quoted }}) < {{ min_value }} or length({{ column_name_quoted }}) > {{ max_value }}
        {% elif min_value is not none %}
            length({{ column_name_quoted }}) < {{ min_value }}
        {% elif max_value is not none %}
            length({{ column_name_quoted }}) > {{ max_value }}
        {% else %}
            1=0
        {% endif %}
    )
)

select *
from validation_errors

{% endtest %}

