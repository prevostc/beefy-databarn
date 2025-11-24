{% macro hex_to_bytes(hex_string) %}
    {#- Converts a hex string (with or without 0x prefix) to bytes -#}
    UNHEX(REPLACE({{ hex_string }}, '0x', ''))
{%- endmacro %}

{% macro normalize_hex_string(hex_string) %}
    {#- Normalizes a hex string to lowercase, adding 0x prefix if it does not have it -#}
    CONCAT('0x', REPLACE(LOWER({{ hex_string }}), '0x', ''))
{%- endmacro %}

{% macro to_decimal(value, scale=20, precision=256) %}
    {#- Converts a value to Decimal with specified precision and scale -#}
    {#- Default: Decimal256 with scale 20 -#}
    {% if precision == 32 %}
        toDecimal32({{ value }}, {{ scale }})
    {% elif precision == 64 %}
        toDecimal64({{ value }}, {{ scale }})
    {% elif precision == 128 %}
        toDecimal128({{ value }}, {{ scale }})
    {% elif precision == 256 %}
        toDecimal256({{ value }}, {{ scale }})
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid precision: " ~ precision ~ ". Must be 32, 64, 128, or 256") }}
    {% endif %}
{%- endmacro %}

