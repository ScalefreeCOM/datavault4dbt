{% macro attribute_standardise() %}
        {{- adapter.dispatch('attribute_standardise', 'datavault4dbt')() -}}
{% endmacro %}

{%- macro default__attribute_standardise() -%}

CONCAT('\"', REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), r'\\', r'\\\\'), r'\[QUOTE]', r'\\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro exasol__attribute_standardise() -%}

{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

CONCAT('"', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS VARCHAR(20000) UTF8 )), '\\\', '\\\\\'), '[QUOTE]', '"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro snowflake__attribute_standardise() -%}

CONCAT('\"', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), '\\', '\\\\'), '\[QUOTE]', '\\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro postgres__attribute_standardise() -%}

CONCAT('"', REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(BOTH ' ' FROM CAST([EXPRESSION] AS VARCHAR)), '\\', '\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '"')

{%- endmacro -%}

{%- macro redshift__attribute_standardise() -%}

'"' ||  REPLACE(REPLACE(REPLACE(TRIM(BOTH ' ' FROM [EXPRESSION]), '\\', '\\\\'), '[QUOTE]', '\\"'), '[NULL_PLACEHOLDER_STRING]', '--') || '"'

{%- endmacro -%}

{%- macro concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, is_hashdiff, rtrim_hashdiff) -%}

{{ return(adapter.dispatch('concattenated_standardise', 'datavault4dbt')(case_sensitive=case_sensitive,
                                                                              hash_alg=hash_alg,
                                                                              datatype=datatype,
                                                                              zero_key=zero_key,
                                                                              alias=alias,
                                                                              is_hashdiff=is_hashdiff,
                                                                              rtrim_hashdiff=rtrim_hashdiff) )}}


{%- endmacro -%}

{%- macro oracle__attribute_standardise() -%}

{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}



 CONCAT('"', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS VARCHAR2(2000) )), '\\\', '\\\\\'), '[QUOTE]', '"'), '[NULL_PLACEHOLDER_STRING]', '--') || '\"')

{%- endmacro -%}






{%- macro default__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, is_hashdiff, rtrim_hashdiff) -%}

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


{%- macro snowflake__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, is_hashdiff, rtrim_hashdiff) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{{ log('datatype: ' ~ datatype, false)}}

{%- if is_hashdiff and rtrim_hashdiff -%}
    {%- set hdiff_prefix = "RTRIM("-%}
    {%- set hdiff_suffix = ",'[NULL_PLACEHOLDER_STRING][CONCAT_STRING]')"-%}
{%- else -%}
    {%- set hdiff_prefix = "" -%}
    {%- set hdiff_suffix = "" -%}
{%- endif -%}

{%- if 'VARCHAR' in datatype or 'CHAR' in datatype or 'STRING' in datatype or 'TEXT' in datatype %}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}({}NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg, hdiff_prefix)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){})), {}) AS {}".format(hdiff_suffix,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){})), {})".format(hdiff_suffix,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}({}NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg, hdiff_prefix)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){})), {}) AS {}".format(hdiff_suffix,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){})), {})".format(hdiff_suffix,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL({}({}NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg, hdiff_prefix)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){}), {}) AS {}".format(hdiff_suffix,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){}), {})".format(hdiff_suffix,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL({}({}NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg,hdiff_prefix)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){}), {}) AS {}".format(hdiff_suffix,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]'){}), {})".format(hdiff_suffix,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro exasol__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias,is_hashdiff, rtrim_hashdiff) -%}

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

{%- macro postgres__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias,is_hashdiff, rtrim_hashdiff) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datatype == 'VARCHAR' -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'))), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'))), {})".format(zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
        {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'))), {}) AS {}".format(zero_key, alias)-%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]')), CAST({} AS {})) AS {}".format(zero_key, datatype, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]')), CAST({} AS {}))".format(zero_key, datatype)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
        {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]')), CAST({} AS {})) AS {}".format(zero_key, datatype, alias)-%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro redshift__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias,is_hashdiff, rtrim_hashdiff) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datatype == 'VARCHAR' -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(".format(hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]'))), {}) AS {}".format(zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]'))), {})".format(zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(".format(hash_alg)-%}
        {%- set standardise_suffix = "\n, '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]'))), {}) AS {}".format(zero_key, alias)-%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(".format(hash_alg)-%}
        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]'))), CAST({} AS {})) AS {}".format(zero_key, datatype, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]'))), CAST({} AS {}))".format(zero_key, datatype)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(".format(hash_alg)-%}
        {%- set standardise_suffix = "\n, '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]'))), CAST({} AS {})) AS {}".format(zero_key, datatype, alias)-%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro oracle__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias,is_hashdiff, rtrim_hashdiff) -%}

{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}



{%- if case_sensitive -%}
    {%- set standardise_prefix = "NULLIF(CAST({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(".format(hash_alg)-%}

    {%- if alias is not none -%}
        {%- set standardise_suffix = "), chr(10), '') , chr(9), ''), chr(11), '') , chr(13), '') AS VARCHAR2(2000)),'[ALL_NULL]')) AS VARCHAR2(40)), {}) AS {} ".format(zero_key, alias) -%}
    {%- else -%}
        {%- set standardise_suffix = "), chr(10), '') , chr(9), ''), chr(11), '') , chr(13), '') AS VARCHAR2(2000)),'[ALL_NULL]')) AS VARCHAR2(40)), {})".format(zero_key, alias) -%}
    {%- endif -%}

{%- else -%}
    {%- set standardise_prefix = "NULLIF(CAST({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(".format(hash_alg) -%}

    {%- if alias is not none -%}
        {%- set standardise_suffix = ", chr(10), '') , chr(9), '') , chr(11), '') , chr(13), '') AS VARCHAR2(2000)), '[ALL_NULL]')) AS VARCHAR2(40)), {}) AS {} ".format(zero_key, alias) -%}
    {%- else %}
        {%- set standardise_suffix = ", chr(10), '') , chr(9), '') , chr(11), '') , chr(13), '') AS VARCHAR2(2000)), '[ALL_NULL]')) AS VARCHAR2(40)), {})".format(zero_key) -%}
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
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL(LOWER({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "IFNULL({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "IFNULL({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {}) AS {}".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\x0B', '') \n, '\\r', '') AS STRING), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {})), {})".format(multi_active_key, main_hashkey_column, ldts_alias, zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro postgres__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}
{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datavault4dbt.is_list(multi_active_key) -%}
    {%- set multi_active_key = multi_active_key|join(", ") -%}
{%- endif -%}

{%- if datatype == 'VARCHAR' -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {}))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {}))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {}))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {}))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {})), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {})), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {})), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]'), ',' ORDER BY {})), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro redshift__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}
{%- set dict_result = {} -%}

{%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

{%- if datavault4dbt.is_list(multi_active_key) -%}
    {%- set multi_active_key = multi_active_key|join(", ") -%}
{%- endif -%}

{%- if datatype == 'VARCHAR' -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {}))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {}))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE(LOWER({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {}))), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {}))), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- else -%}

    {%- if case_sensitive -%}
        {%- set standardise_prefix = "COALESCE({}(STRING_AGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {})), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {})), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- else -%}
        {%- set standardise_prefix = "COALESCE({}(STRING_AGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {})), {}) AS {}".format(multi_active_key,zero_key, alias)-%}
        {%- else -%}
            {%- set standardise_suffix = "\n), '\\\\n', '') \n, '\\\\t', '') \n, '\\\\v', '') \n, '\\\\r', '') AS VARCHAR), '[ALL_NULL]')) within group (ORDER BY {})), {})".format(multi_active_key,zero_key)-%}
        {%- endif -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro oracle__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column) -%}

    {%- set dict_result = {} -%}

    {%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

    {%- if multi_active_key is not string and multi_active_key is iterable -%}
        {%- set multi_active_key = multi_active_key|join(", ") -%}
    {%- endif -%}
    {%- if case_sensitive -%}
        {%- set standardise_prefix = "NULLIF({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000),'[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {}) AS {} ".format(multi_active_key, zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000),'[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {})".format(multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- else -%}
        {%- set standardise_prefix = "NULLIF({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(".format(hash_alg) -%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {}) AS {} ".format(multi_active_key, zero_key , alias) -%}
        {%- else %}
            {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000), '[ALL_NULL]')) WITHIN GROUP (ORDER BY {})), {})".format(multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- endif -%}
    {%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

    {{ return(dict_result | tojson ) }}

{%- endmacro -%}