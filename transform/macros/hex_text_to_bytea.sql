
{% macro hex_text_to_bytea(column_name) %}
    decode(substring("{{ column_name }}", 3), 'hex')
{% endmacro %}
