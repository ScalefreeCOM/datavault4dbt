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

CONCAT('\'', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), '\\', '\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\'')

{%- endmacro -%}


{%- macro concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}

{{ return(adapter.dispatch('concattenated_standardise', 'datavault4dbt')(case_sensitive=case_sensitive,
                                                                              hash_alg=hash_alg,
                                                                              all_null=all_null,
                                                                              zero_key=zero_key,
                                                                              alias=alias) )}}

{%- endmacro -%}

{%- macro default__concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}

{%- set dict_result = {} -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro snowflake__concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}

{%- set dict_result = {} -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '{}'))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '{}'))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}

{%- macro exasol__concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}
{%- set dict_result = {} -%}
{%- if case_sensitive -%}
    {%- set standardise_prefix = "NULLIF({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

    {%- if alias is not none -%}
        {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'{}')), '{}') AS {} ".format(all_null | join(""), zero_key, alias) -%}
    {%- else -%}
        {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'{}')), '{}')".format(all_null | join(""), zero_key, alias) -%}
    {%- endif -%}

{%- else -%}
    {%- set standardise_prefix = "NULLIF({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(".format(hash_alg) -%}

    {%- if alias is not none -%}
        {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '{}')), '{}') AS {} ".format(all_null | join(""), zero_key , alias) -%}
    {%- else %}
        {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '{}')), '{}')".format(all_null | join(""), zero_key) -%}
    {%- endif -%}

{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}










{%- macro multi_active_concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias, multi_active_key, main_hashkey_column) -%}

{{ adapter.dispatch('multi_active_concattenated_standardise', 'datavault4dbt')(case_sensitive=case_sensitive,
                                                                              hash_alg=hash_alg,
                                                                              all_null=all_null,
                                                                              zero_key=zero_key,
                                                                              alias=alias,
                                                                              multi_active_key=multi_active_key,
                                                                              main_hashkey_column=main_hashkey_column) }}

{%- endmacro -%}

{%- macro default__multi_active_concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias, multi_active_key, main_hashkey_column) -%}
{%- set dict_result = {} -%}

{%- if datavault4dbt.is_list(multi_active_key) -%}
    {%- set multi_active_key = multi_active_key|join(", ") -%}
{%- endif -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}') ORDER BY {})))), '{}') AS {}".format(all_null | join(""),multi_active_key,zero_key, alias)-%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(STRING_AGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}') ORDER BY {})))), '{}') AS {}".format(all_null | join(""),multi_active_key,zero_key, alias)-%}
{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}


{%- macro exasol__multi_active_concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias, multi_active_key, main_hashkey_column) -%}

    {%- set dict_result = {} -%}

    {%- if multi_active_key is not string and multi_active_key is iterable -%}
        {%- set multi_active_key = multi_active_key|join(", ") -%}
    {%- endif -%}
    {%- if case_sensitive -%}
        {%- set standardise_prefix = "NULLIF({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(CONCAT(".format(hash_alg)-%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'{}')) WITHIN GROUP (ORDER BY {})), '{}') AS {} ".format(all_null | join(""), multi_active_key, zero_key, alias) -%}
        {%- else -%}
            {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'{}')) WITHIN GROUP (ORDER BY {})), '{}')".format(all_null | join(""), multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- else -%}
        {%- set standardise_prefix = "NULLIF({}(LISTAGG(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(".format(hash_alg) -%}

        {%- if alias is not none -%}
            {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '{}')) WITHIN GROUP (ORDER BY {})), '{}') AS {} ".format(all_null | join(""), multi_active_key, zero_key , alias) -%}
        {%- else %}
            {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '{}')) WITHIN GROUP (ORDER BY {})), '{}')".format(all_null | join(""),  multi_active_key, zero_key) -%}
        {%- endif -%}

    {%- endif -%}
    {%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

    {{ return(dict_result | tojson ) }}
    
{%- endmacro -%}


{%- macro snowflake__multi_active_concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias, multi_active_key, main_hashkey_column) -%}

{%- set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') -%}

{%- set dict_result = {} -%}

{%- if datavault4dbt.is_list(multi_active_key) -%}
    {%- set multi_active_key = multi_active_key|join(", ") -%}
{%- endif -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(LOWER({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '{}')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), '{}') AS {}".format(all_null | join(""), multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(LOWER({}(LISTAGG(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS STRING), '{}')) WITHIN GROUP (ORDER BY {}) OVER (PARTITION BY {}, {}))), '{}') AS {}".format(all_null | join(""), multi_active_key, main_hashkey_column, ldts_alias, zero_key, alias)-%}
{%- endif -%}

{%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}

{{ return(dict_result | tojson ) }}

{%- endmacro -%}
