{% test is_bytes_string(model, column_name, bytes_length=None) %}

{%- set column_name_quoted = adapter.quote(column_name) if column_name else 'col' %}

with validation_errors as (
    select
        {{ column_name_quoted }}
    from {{ model }}
    where not (
        match(hex({{ column_name_quoted }}), '^([0-9a-fA-F]{2})+$')
        {% if bytes_length is not none %}
            and length({{ column_name_quoted }}) = {{ bytes_length }}
        {% endif %}
    )
)

select *
from validation_errors

{% endtest %}

