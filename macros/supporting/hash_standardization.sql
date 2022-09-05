{% macro attribute_standardise() %}
        {{- adapter.dispatch('attribute_standardise', 'dbtvault_scalefree')() -}}
{% endmacro %}

{%- macro default__attribute_standardise() -%}

CONCAT('\"', REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), r'\\', r'\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro snowflake__attribute_standardise() -%}

CONCAT('\"', REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), r'\\', r'\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro exasol__attribute_standardise() -%}

{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

CONCAT('"', REPLACE(REPLACE(REPLACE(TRIM(CAST([EXPRESSION] AS VARCHAR(20000) UTF8 )), '\\\', '\\\\\'), '[QUOTE]', '"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')

{%- endmacro -%}

{%- macro concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}

{{ return(adapter.dispatch('concattenated_standardise', 'dbtvault_scalefree')(case_sensitive=case_sensitive,
                                                                              hash_alg=hash_alg,
                                                                              all_null=all_null,
                                                                              zero_key=zero_key,
                                                                              alias=alias) )}}

{%- endmacro -%}

{%- macro default__concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- endif -%}

{{ return((standardise_prefix, standardise_suffix)) }}

{%- endmacro -%}


{%- macro snowflake__concattenated_standardise(case_sensitive, hash_alg, all_null, zero_key, alias) -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""),zero_key, alias)-%}
{%- endif -%}

{{ return((standardise_prefix, standardise_suffix)) }}

{%- endmacro -%}
