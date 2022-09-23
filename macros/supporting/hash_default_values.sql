{%- macro hash_default_values(hash_function, hash_datatype) -%}

    {{ return(adapter.dispatch('hash_default_values', 'dbtvault_scalefree')(hash_function=hash_function,hash_datatype=hash_datatype)) }}

{%- endmacro -%}

{%- macro default__hash_default_values(hash_function, hash_datatype) -%}

    {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = '00000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA' or hash_function == 'SHA1' and hash_datatype == 'STRING' -%}
        {%- set hash_alg = 'SHA1' -%}
        {%- set unknown_key = '0000000000000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA256' and hash_datatype == 'STRING' -%}
        {%- set hash_alg = 'SHA256' -%}
        {%- set unknown_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {{ return([hash_alg, unknown_key, error_key]) }}

{%- endmacro -%}


{%- macro snowflake__hash_default_values(hash_function, hash_datatype) -%}

    {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = "REPEAT('0',32)" -%}
        {%- set error_key = "REPEAT('f',32)" -%}
    {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' -%} 
        {%- if hash_datatype == 'STRING' -%}
            {%- set hash_alg = 'SHA1' -%}
            {%- set unknown_key = "REPEAT('0',40)" -%}
            {%- set error_key = "REPEAT('f',40)" -%}
        {%- elif hash_datatype == 'BINARY' -%}
            {%- set hash_alg = 'SHA1_BINARY' -%}
            {%- set unknown_key = "TO_BINARY(REPEAT('0',40))" -%}
            {%- set error_key = "TO_BINARY(REPEAT('f',40))" -%}        
        {%- endif -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
        {%- if hash_datatype == 'STRING' -%}
            {%- set hash_alg = 'SHA2' -%}
            {%- set unknown_key = "REPEAT('0',64)" -%}
            {%- set error_key = "REPEAT('f',64)" -%}
        {%- elif hash_datatype == 'BINARY' -%}
            {%- set hash_alg = 'SHA2_BINARY' -%}
            {%- set unknown_key = "TO_BINARY(REPEAT('0',64))" -%}
            {%- set error_key = "TO_BINARY(REPEAT('f',64))" -%}        
        {%- endif -%}            
    {%- endif -%}

    {{ return([hash_alg, unknown_key, error_key]) }}

{%- endmacro -%}
