{% macro attribute_standardise() %}
        {{- adapter.dispatch('attribute_standardise', 'datavault4dbt')() -}}
{% endmacro %}

{%- macro default__attribute_standardise() -%}

CONCAT('\"', REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), r'\\', r'\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro exasol__attribute_standardise() -%}

{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

CONCAT('"', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS VARCHAR(20000) UTF8 )), '\\\', '\\\\\'), '[QUOTE]', '"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro snowflake__attribute_standardise() -%}

CONCAT('\"', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), '\\', '\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}


{%- macro sqlserver__attribute_standardise() -%}

REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8), '[QUOTE]', '"'), '[NULL_PLACEHOLDER_STRING]', '--')

{%- endmacro -%}



{%- macro concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias) -%}

{{ return(adapter.dispatch('concattenated_standardise', 'datavault4dbt')(case_sensitive=case_sensitive,
                                                                              hash_alg=hash_alg,
                                                                              datatype=datatype, 
                                                                              zero_key=zero_key,
                                                                              alias=alias) )}}

{%- endmacro -%}

{%- macro default__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datatype == 'STRING' -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]')))), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]')))), {})".format(zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
        {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]')))), {}) AS {}".format(zero_key, alias)-%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]')), CAST({} AS {})) AS {}".format(zero_key, datatype, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]')), CAST({} AS {}))".format(zero_key, datatype)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
        {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]')), CAST({} AS {})) AS {}".format(zero_key, datatype, alias)-%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro snowflake__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{{ log('datatype: ' ~ datatype, false)}}

{%- if 'VARCHAR' in datatype or 'CHAR' in datatype or 'STRING' in datatype or 'TEXT' in datatype %}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'))), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'))), {})".format(zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'))), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'))), {})".format(zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')), {})".format(zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')), {})".format(zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro exasol__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "NULLIF({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

    {%- if alias is not none -%}
        {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'[ALL_NULL]')), {}) AS {} ".format(zero_key, alias) -%}
    {%- else -%}
        {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'[ALL_NULL]')), {})".format(zero_key, alias) -%}
    {%- endif -%}

{%- else -%}
    {%- set standardise_prefix = "NULLIF({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(".format(hash_alg) -%}

    {%- if alias is not none -%}
        {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '[ALL_NULL]')), {}) AS {} ".format(zero_key, alias) -%}
    {%- else %}
        {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '[ALL_NULL]')), {})".format(zero_key) -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro sqlserver__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias) -%}

{%- set dict_result = {} -%}


{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if 'VARCHAR' in datatype or 'CHAR' in datatype %}

    {# 
        set to 1, if the binary->char hash conversion should be prefixed with 0x... 
        set to 2, if the binary->char hash conversion should exclude the 0x prefix
    #}
    {%- set convert_to_hex_style = 2 -%} 


    {%- if case_sensitive -%}
        {%- set standardise_prefix = "CONVERT({}, ISNULL(LOWER(CONVERT({}, HASHBYTES('{}', (NULLIF(CAST(CONCAT(".format(datatype,datatype, hash_alg)-%}
        {%- if alias is not none -%}    
            {%- set standardise_suffix = ") AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8, '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8),{})), {})) AS {}".format(convert_to_hex_style,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = ") AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8, '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8),{})), {}))".format(convert_to_hex_style,zero_key)-%}
        {%- endif -%}    
    {%- else -%}

        {%- set standardise_prefix = "CONVERT({}, ISNULL(LOWER(CONVERT({}, HASHBYTES('{}', (NULLIF(CAST(UPPER(CONCAT(".format(datatype,datatype, hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = ")) AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8 , '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8),{})), {})) AS {}".format(convert_to_hex_style,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = ")) AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8, '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8),{})), {}))".format(convert_to_hex_style,zero_key)-%}
        {%- endif -%}
    {%- endif -%}
{%- else -%}
    {%- if case_sensitive -%}
        {%- set standardise_prefix = "CONVERT({}, ISNULL(HASHBYTES('{}', (NULLIF(CAST(CONCAT(".format(datatype, hash_alg)-%}
        {%- if alias is not none -%}    
            {%- set standardise_suffix = ") AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8, '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8), {})) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = ") AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8, '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8), {}))".format(zero_key)-%}
        {%- endif -%}    

    {%- else -%}

        {%- set standardise_prefix = "CONVERT({}, ISNULL(HASHBYTES('{}', (NULLIF(CAST(UPPER(CONCAT(".format(datatype, hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = ")) AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8 , '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8), {})) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = ")) AS varchar(max)) COLLATE Latin1_General_100_BIN2_UTF8, '[ALL_NULL]')) COLLATE Latin1_General_100_BIN2_UTF8), {}))".format(zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}









{%- macro multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}

{{ adapter.dispatch('multi_active_concattenated_standardise', 'datavault4dbt')(case_sensitive=case_sensitive,
                                                                              hash_alg=hash_alg,
                                                                              datatype=datatype, 
                                                                              zero_key=zero_key,
                                                                              alias=alias,
                                                                              multi_active_key=multi_active_key,
                                                                              main_hashkey_column=main_hashkey_column) }}

{%- endmacro -%}

{%- macro default__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}
{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datavault4dbt.is_list(multi_active_key) -%}
    {%- set multi_active_key = multi_active_key|join(", ") -%}
{%- endif -%}

{%- if datatype == 'STRING' -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {})))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {})))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {})))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {})))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL(TO_HEX({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {}))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {}))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(TO_HEX({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {}))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '[ALL_NULL]') ORDER BY {}))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro exasol__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}

    {%- set dict_result = {} -%}
    
    {%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

    {%- if multi_active_key is not string and multi_active_key is iterable -%}
        {%- set multi_active_key = multi_active_key|join(", ") -%}
    {%- endif -%}
    {%- if case_sensitive -%}
        {%- set standardise_prefix = "NULLIF({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {}) AS {} ".format(multi_active_key, zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {})".format(multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- else -%}
        {%- set standardise_prefix = "NULLIF({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(".format(hash_alg) -%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {}) AS {} ".format(multi_active_key, zero_key , alias) -%}
        {%- else %}
            {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {})".format(multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- endif -%}
    {%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

    {{ return(dict_result | tojson ) }}
    
{%- endmacro -%}


{%- macro snowflake__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}

{%- set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datavault4dbt.is_list(multi_active_key) -%}
    {%- set multi_active_key = multi_active_key|join(", ") -%}
{%- endif -%}


{%- if 'VARCHAR' in datatype or 'CHAR' in datatype or 'STRING' in datatype or 'TEXT' in datatype %}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro sqlserver__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}

    {%- set dict_result = {} -%}

    {%- set concat_string = var('concat_string', '||') -%}
    {%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

    
    {%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

    {%- if multi_active_key is not string and multi_active_key is iterable -%}
        {%- set multi_active_key = multi_active_key|join(", ") -%}
    {%- endif -%}

{%- if 'VARCHAR' in datatype or 'CHAR' in datatype %}

    {# 
        set to 1, if the binary->char hash conversion should be prefixed with 0x... 
        set to 2, if the binary->char hash conversion should exclude the 0x prefix
    #}
    {%- set convert_to_hex_style = 2 -%} 

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "CONVERT({}, ISNULL(LOWER(CONVERT({}, HASHBYTES('{}',STRING_AGG(REPLACE(CAST(CONCAT(".format(datatype,datatype, hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ",'') AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8),{})), {})) AS {} ".format(null_placeholder_string,concat_string,multi_active_key, convert_to_hex_style,zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ",'') AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8),{})), {})) ".format(null_placeholder_string,concat_string,multi_active_key, convert_to_hex_style,zero_key) -%}
        {%- endif -%}

    {%- else -%}
        {%- set standardise_prefix = "CONVERT({}, ISNULL(LOWER(CONVERT({}, HASHBYTES('{}',STRING_AGG(REPLACE(CAST(UPPER(CONCAT(".format(datatype,datatype, hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ",'')) AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8),{})), {})) AS {} ".format(null_placeholder_string,concat_string,multi_active_key, convert_to_hex_style,zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ",'')) AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8),{})), {})) ".format(null_placeholder_string,concat_string,multi_active_key, convert_to_hex_style,zero_key) -%}
        {%- endif -%}

    {%- endif -%}
{%- else -%} 
    {%- if case_sensitive -%}
        {%- set standardise_prefix = "CONVERT({}, ISNULL(HASHBYTES('{}',STRING_AGG(REPLACE(CAST(CONCAT(".format(datatype,hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ",'') AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8), {})) AS {} ".format(null_placeholder_string,concat_string,multi_active_key, zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ",'') AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8), {})) ".format(null_placeholder_string,concat_string,multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- else -%}
        {%- set standardise_prefix = "CONVERT({}, ISNULL(HASHBYTES('{}',STRING_AGG(REPLACE(CAST(UPPER(CONCAT(".format(datatype,hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ",'')) AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8), {})) AS {} ".format(null_placeholder_string,concat_string,multi_active_key, zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ",'')) AS VARCHAR(max)) COLLATE Latin1_General_100_BIN2_UTF8,'[ALL_NULL]','{}'),'{}')  WITHIN GROUP (ORDER BY {}) COLLATE Latin1_General_100_BIN2_UTF8), {})) ".format(null_placeholder_string,concat_string,multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- endif -%}

{%- endif -%}

    {%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

    {{ return(dict_result | tojson ) }}
    
{%- endmacro -%}
