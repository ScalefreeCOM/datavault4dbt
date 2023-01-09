{%- macro hash_default_values(hash_function, hash_datatype=none) -%}

    {{ return(adapter.dispatch('hash_default_values', 'datavault4dbt')(hash_function=hash_function,hash_datatype=hash_datatype)) }}

{%- endmacro -%}

{%- macro default__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}

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

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro snowflake__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}

    {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = "00000000000000000000000000000000" -%}
        {%- set error_key = "ffffffffffffffffffffffffffffffff" -%}
    {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' -%} 
        {%- if hash_datatype == 'STRING' -%}
            {%- set hash_alg = 'SHA1' -%}
            {%- set unknown_key = "0000000000000000000000000000000000000000" -%}
            {%- set error_key = "ffffffffffffffffffffffffffffffffffffffff" -%}
        {%- elif hash_datatype == 'BINARY' -%}
            {%- set hash_alg = 'SHA1_BINARY' -%}
            {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000')" -%}
            {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffff')" -%}        
        {%- endif -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
        {%- if hash_datatype == 'STRING' -%}
            {%- set hash_alg = 'SHA2' -%}
            {%- set unknown_key = "0000000000000000000000000000000000000000000000000000000000000000" -%}
            {%- set error_key = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" -%}
        {%- elif hash_datatype == 'BINARY' -%}
            {%- set hash_alg = 'SHA2_BINARY' -%}
            {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000000000000000000000000000')" -%}
            {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')" -%}        
        {%- endif -%}   
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro exasol__hash_default_values(hash_function, hash_datatype=none) -%}

    {%- set dict_result = {} -%}

    {%- if hash_function == 'MD5' -%}
        {%- set hash_alg = 'HASHTYPE_MD5' -%}
        {%- set unknown_key = '00000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffff' -%}
    {%- elif (hash_function == 'SHA' or hash_function == 'SHA1') -%}
        {%- set hash_alg = 'HASHTYPE_SHA1' -%}
        {%- set unknown_key = '0000000000000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif (hash_function == 'SHA2' or hash_function == 'SHA256') -%}
        {%- set hash_alg = 'HASHTYPE_SHA256' -%}
        {%- set unknown_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}


{%- endmacro -%}