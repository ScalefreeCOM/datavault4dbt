{%- macro hash_default_values(hash_function, hash_datatype=none) -%}

    {{ return(adapter.dispatch('hash_default_values', 'datavault4dbt')(hash_function=hash_function,hash_datatype=hash_datatype)) }}

{%- endmacro -%}

{%- macro default__hash_default_values(hash_function, hash_datatype) -%}

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


{%- macro snowflake__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}


    {{ log('hash datatype: ' ~ hash_datatype, false) }}

    {%- if hash_function == 'MD5' and hash_datatype == 'STRING' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = '!00000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffff' -%}
    {%- elif (hash_function == 'MD5' or hash_function == 'MD5_BINARY') and hash_datatype == 'BINARY' -%}
        {%- set hash_alg = 'MD5_BINARY' -%}
        {%- set unknown_key = "TO_BINARY('00000000000000000000000000000000')" -%}
        {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffff')" -%}
    {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' or hash_function == 'SHA' -%} 
        {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
            {%- set hash_alg = 'SHA1' -%}
            {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
            {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
        {%- elif 'BINARY' in hash_datatype -%}
            {%- set hash_alg = 'SHA1_BINARY' -%}
            {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000')" -%}
            {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffff')" -%}        
        {%- endif -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
        {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
            {%- set hash_alg = 'SHA2' -%}
            {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
            {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
        {%- elif 'BINARY' in hash_datatype -%}
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
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}

    {%- if 'MD5' in hash_function -%}

        {%- if 'HASHTYPE' in hash_datatype -%}
            {%- set hash_alg = 'HASHTYPE_MD5' -%}
        {%- elif 'CHAR' in hash_datatype -%}
            {%- set hash_alg = 'HASH_MD5' -%}
        {%- endif -%}

        {%- set unknown_key = '!00000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffff' -%}
    {%- elif (hash_function == 'SHA' or hash_function == 'SHA1') -%}

        {%- if 'HASHTYPE' in hash_datatype -%}
            {%- set hash_alg = 'HASHTYPE_SHA1' -%}
        {%- elif 'CHAR' in hash_datatype -%}
            {%- set hash_alg = 'HASH_SHA1' -%}
        {%- endif -%}

        {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif (hash_function == 'SHA2' or hash_function == 'SHA256') -%}

        {%- if 'HASHTYPE' in hash_datatype -%}
            {%- set hash_alg = 'HASHTYPE_SHA256' -%}
        {%- elif 'CHAR' in hash_datatype -%}
            {%- set hash_alg = 'HASH_SHA256' -%}
        {%- endif -%}
        
        {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}


{%- endmacro -%}


{%- macro synapse__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}


    {{ log('hash datatype: ' ~ hash_datatype, false) }}

    {%- if hash_function == 'MD5' -%}
        {%- if 'VARCHAR' in hash_datatype|upper or 'CHAR' in hash_datatype|upper or 'STRING' in hash_datatype|upper or 'TEXT' in hash_datatype|upper %}
            {%- set hash_alg = 'MD5' -%}
            {%- set unknown_key = "CONVERT(varchar(34), '00000000000000000000000000000000')" -%}
            {%- set error_key = "CONVERT(varchar(34), 'ffffffffffffffffffffffffffffffff')" -%}
        {%- elif 'BINARY' in hash_datatype|upper %}
            {%- set hash_alg = 'MD5' -%}
            {%- set unknown_key = "CONVERT(binary(16), '00000000000000000000000000000000')" -%}
            {%- set error_key = "CONVERT(binary(16), 'ffffffffffffffffffffffffffffffff')" -%}           
        {%- endif -%} 
    {%- elif hash_function == 'SHA1' or hash_function == 'SHA1_HEX' or hash_function == 'SHA' -%} 
        {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
            {%- set hash_alg = 'SHA1' -%}
            {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
            {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
        {%- elif 'BINARY' in hash_datatype -%}
            {%- set hash_alg = 'SHA1_BINARY' -%}
            {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000')" -%}
            {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffff')" -%}        
        {%- endif -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_HEX' -%}
        {%- if 'VARCHAR' in hash_datatype or 'CHAR' in hash_datatype or 'STRING' in hash_datatype or 'TEXT' in hash_datatype %}
            {%- set hash_alg = 'SHA2' -%}
            {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
            {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
        {%- elif 'BINARY' in hash_datatype -%}
            {%- set hash_alg = 'SHA2_BINARY' -%}
            {%- set unknown_key = "TO_BINARY('0000000000000000000000000000000000000000000000000000000000000000')" -%}
            {%- set error_key = "TO_BINARY('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')" -%}        
        {%- endif -%}   
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro fabric__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}


    {{ log('hash datatype: ' ~ hash_datatype, false) }}

    {%- if hash_function == 'MD5' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = "CONVERT("~hash_datatype~", '00000000000000000000000000000000')" -%}
        {%- set error_key = "CONVERT("~hash_datatype~", 'ffffffffffffffffffffffffffffffff')" -%}
    {%- elif hash_function == 'SHA1' or hash_function == 'SHA' -%} 
        {%- set hash_alg = 'SHA1' -%}
        {%- set unknown_key = "CONVERT("~hash_datatype~",'0000000000000000000000000000000000000000')" -%}
        {%- set error_key = "CONVERT("~hash_datatype~",'ffffffffffffffffffffffffffffffffffffffff')" -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA2_256' -%}
        {%- set hash_alg = 'SHA2_256 ' -%}
        {%- set unknown_key = "CONVERT("~hash_datatype~",'0000000000000000000000000000000000000000000000000000000000000000')" -%}
        {%- set error_key = "CONVERT("~hash_datatype~",'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')" -%}
    {%- elif hash_function == 'SHA2_512' -%}
        {%- set hash_alg = 'SHA2_512 ' -%}
        {%- set unknown_key = "CONVERT("~hash_datatype~",'00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')" -%}
        {%- set error_key = "CONVERT("~hash_datatype~",'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff')" -%}
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro redshift__hash_default_values(hash_function, hash_datatype) -%}

    {%- set dict_result = {} -%}
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}
    {%- set hash_bits = '' -%}

    {%- if hash_function == 'MD5' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = '!00000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA' or hash_function == 'SHA1' -%}
        {%- set hash_alg = 'SHA1' -%}
        {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA256' -%}
        {%- set hash_alg = 'SHA2' -%}
        {%- set hash_bits = ', 256' -%}
        {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA512' -%}
        {%- set hash_alg = 'SHA2' -%}
        {%- set hash_bits = ', 512' -%}
        {%- set unknown_key = '!00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key, "hash_bits": hash_bits }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro databricks__hash_default_values(hash_function, hash_datatype) -%}

  {%- set dict_result = {} -%}
    {%- set hash_alg = '' -%}
    {%- set unknown_key = '' -%}
    {%- set error_key = '' -%}
    {%- set hash_bits = '' -%}

    {%- if hash_function == 'MD5' -%}
        {%- set hash_alg = 'MD5' -%}
        {%- set unknown_key = '!00000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA' or hash_function == 'SHA1' -%}
        {%- set hash_alg = 'SHA1' -%}
        {%- set unknown_key = '!0000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA2' or hash_function == 'SHA256' -%}
        {%- set hash_alg = 'SHA2' -%}
        {%- set hash_bits = ', 256' -%}
        {%- set unknown_key = '!0000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- elif hash_function == 'SHA512' -%}
        {%- set hash_alg = 'SHA2' -%}
        {%- set hash_bits = ', 512' -%}
        {%- set unknown_key = '!00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' -%}
        {%- set error_key = '!ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' -%}
    {%- endif -%}

    {%- do dict_result.update({"hash_alg": hash_alg, "unknown_key": unknown_key, "error_key": error_key, "hash_bits": hash_bits }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}