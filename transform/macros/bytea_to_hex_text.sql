
{% macro bytea_to_hex_text(column_name) %}
    '0x' || encode("{{ column_name }}"::bytea, 'hex')
{% endmacro %}
