{% macro evm_address(hex_string) %}
    UNHEX(REPLACE({{ hex_string }}, '0x', ''))
{%- endmacro %}

{% macro evm_transaction_hash(hex_string) %}
    UNHEX(REPLACE({{ hex_string }}, '0x', ''))
{%- endmacro %}

{% macro format_hex(hex_string) %}
    '0x' || lower(hex({{ hex_string }}))
{%- endmacro %}

{% macro normalize_network_beefy_key(network) %}
    assumeNotNull(replace(lower(trim({{ network }})), 'harmony', 'one'))
{%- endmacro %}

{% macro representation_native_evm_address() %}
    assumeNotNull(unhex('0000000000000000000000000000000000000000'))
{%- endmacro %}

{% macro to_representation_evm_address(hex_string) %}
    case 
        when length({{ hex_string }}) = 20 then assumeNotNull({{ hex_string }})
        when trim(lower({{ hex_string }})) = 'null' then {{ representation_native_evm_address() }} 
        when {{ hex_string }} is null then {{ representation_native_evm_address() }}
        else assumeNotNull({{ evm_address(hex_string) }})
    end
{%- endmacro %}

-- {% macro normalize_hex_string(hex_string) %}
--     {#- Normalizes a hex string to lowercase, adding 0x prefix if it does not have it -#}
--     CONCAT('0x', REPLACE(LOWER({{ hex_string }}), '0x', ''))
-- {%- endmacro %}

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

