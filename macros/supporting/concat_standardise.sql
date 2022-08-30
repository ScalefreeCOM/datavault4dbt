{%- macro concat_standardise(hash_alg, all_null, case_sensitive=false, unknown_key=none, alias=none) -%}


    {{- adapter.dispatch('concat_standardise', 'dbtvault_scalefree')(case_sensitive=case_sensitive,
                                             hash_alg=hash_alg,
                                             all_null= all_null,
                                             unknown_key=unknown_key,
                                             alias=alias) -}}

{%- endmacro %}

{%- macro default__concat_standardise(case_sensitive, hash_alg, all_null, unknown_key, alias) -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n)), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {} ".format(all_null | join(""),unknown_key, alias) -%}
{%- else -%}
    {%- set standardise_prefix = "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg)-%}
    {%- set standardise_suffix = "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') ".format(all_null | join(""),unknown_key) -%}
{%- endif -%}

{{ return((standardise_prefix, standardise_suffix)) }}

{%- endmacro -%}

{%- macro exasol__concat_standardise(case_sensitive, hash_alg, all_null, unknown_key, alias) -%}

{%- if case_sensitive -%}
    {%- set standardise_prefix = "NULLIF({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(CONCAT(".format(hash_alg) | string-%}
    {%- set standardise_suffix = ")), char(10), '') , char(9), ''), char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8),'{}')), '{}') AS {} ".format(all_null | join(""),unknown_key, alias) -%}
{%- else -%}
    {%- set standardise_prefix = "NULLIF({}(NULLIF(CAST(REPLACE(REPLACE(REPLACE(REPLACE(CONCAT(".format(hash_alg) | string -%}
    {{ log("std prefix in concat macro: " ~ standardise_prefix, true )}}
    {%- set standardise_suffix = "), char(10), '') , char(9), '') , char(11), '') , char(13), '') AS VARCHAR(2000000) UTF8), '{}')), '{}')".format(all_null | join(""),unknown_key) -%}
    {{ log("std suffix in concat macro: " ~ standardise_suffix, true) }}
{%- endif -%}

{{ return((standardise_prefix, standardise_suffix)) }}

{%- endmacro -%}
