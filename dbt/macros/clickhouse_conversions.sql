{% macro evm_address(hex_string) %}
    lower({{ hex_string }})
{%- endmacro %}

{% macro evm_transaction_hash(hex_string) %}
    lower({{ hex_string }})
{%- endmacro %}

{% macro format_hex(hex_string) %}
    '0x' || lower(hex({{ hex_string }}))
{%- endmacro %}

{% macro normalize_network_beefy_key(network) %}
    assumeNotNull(replace(lower(trim({{ network }})), 'harmony', 'one'))
{%- endmacro %}

{% macro representation_native_evm_address() %}
    assumeNotNull('0x0000000000000000000000000000000000000000')
{%- endmacro %}

{% macro to_representation_evm_address(hex_string) %}
    case 
        --when length({{ hex_string }}) = 20 then assumeNotNull({{ hex_string }})
        when {{ hex_string }} is null then {{ representation_native_evm_address() }}
        when trim(lower({{ hex_string }})) = 'null' then {{ representation_native_evm_address() }} 
        when trim(lower({{ hex_string }})) = '' then {{ representation_native_evm_address() }}
        else assumeNotNull({{ evm_address(hex_string) }})
    end
{%- endmacro %}

-- {% macro normalize_hex_string(hex_string) %}
--     {#- Normalizes a hex string to lowercase, adding 0x prefix if it does not have it -#}
--     CONCAT('0x', REPLACE(LOWER({{ hex_string }}), '0x', ''))
-- {%- endmacro %}

{% macro to_decimal(value) %}
    {#- Converts a value to Decimal with specified precision and scale -#}
    toDecimal256({{ value }}, 20)
{%- endmacro %}

