{%- macro trino__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}

    {%- if hash_function == 'MD5' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = '!00000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA' or hash_function == 'SHA1' -%}
        {%- set hash_alg = 'SHA1' -%}
        {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA256' -%}
        {%- set hash_alg = 'SHA256' -%}
        {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}