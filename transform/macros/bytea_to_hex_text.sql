
{% macro bytea_to_hex_text(table_name, column_name) %}
    '0x' || encode("{{ table_name }}"."{{ column_name }}"::bytea, 'hex')
{% endmacro %}
