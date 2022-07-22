{%- macro hash_default_values(hash_function) -%}

    {{ return(adapter.dispatch('hash_default_values', 'dbtvault_scalefree')(hash_function=hash_function)) }}

{%- endmacro -%}

{%- macro default__hash_default_values(hash_function) -%}

    {%- if hash_function == 'MD5' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = '00000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA' or hash_function == 'SHA1' -%}
        {%- set hash_alg = 'SHA1' -%}
        {%- set unknown_key = '0000000000000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA256' -%}
        {%- set hash_alg = 'SHA256' -%}
        {%- set unknown_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {{ return([hash_alg, unknown_key, error_key]) }}

{%- endmacro -%}