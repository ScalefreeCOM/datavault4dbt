{%- macro trino__attribute_standardise(hash_type, use_trim) -%}
    {%- set expr = "TRIM(CAST([EXPRESSION] AS VARCHAR))" if use_trim else "CAST([EXPRESSION] AS VARCHAR)" -%}
    CONCAT('\"', REPLACE(REGEXP_REPLACE(REGEXP_REPLACE({{ expr }}, '\\\\', '\\\\\\\\'), '[QUOTE]', '\"'), '[NULL_PLACEHOLDER_STRING]', '--'), '\"')
{%- endmacro -%}

{%- macro trino__concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, is_hashdiff, rtrim_hashdiff) -%}
    {%- set dict_result = {} -%}
    {%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

    {%- if is_hashdiff and rtrim_hashdiff -%}
        {%- set hdiff_prefix = "RTRIM(" -%}
        {%- set hdiff_suffix = ",'[NULL_PLACEHOLDER_STRING][CONCAT_STRING]')" -%}
    {%- else -%}
        {%- set hdiff_prefix = "" -%}
        {%- set hdiff_suffix = "" -%}
    {%- endif -%}

    {%- set standardise_prefix = "COALESCE(TO_HEX(" ~ hash_alg ~ "(TO_UTF8(" ~ hdiff_prefix ~ "NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(" -%}
    
    {%- if not case_sensitive -%}
        {%- set standardise_prefix = standardise_prefix ~ "UPPER(CONCAT(" -%}
    {%- else -%}
        {%- set standardise_prefix = standardise_prefix ~ "CONCAT(" -%}
    {%- endif -%}

    {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]') " ~ hdiff_suffix ~ "))), CAST(" ~ zero_key ~ " AS VARCHAR))" -%}
    
    {%- if alias is not none -%}
        {%- set standardise_suffix = standardise_suffix ~ " AS " ~ alias -%}
    {%- endif -%}

    {%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}
    {{ return(dict_result | tojson ) }}
{%- endmacro -%}

{%- macro trino__multi_active_concattenated_standardise(case_sensitive, hash_alg, datatype, zero_key, alias, multi_active_key, main_hashkey_column, is_hashdiff, rtrim_hashdiff) -%}
    {%- set dict_result = {} -%}
    {%- set zero_key = datavault4dbt.as_constant(column_str=zero_key) -%}

    {%- if is_hashdiff and rtrim_hashdiff -%}
        {%- set hdiff_prefix = "RTRIM(" -%}
        {%- set hdiff_suffix = ",'[NULL_PLACEHOLDER_STRING][CONCAT_STRING]')" -%}
    {%- else -%}
        {%- set hdiff_prefix = "" -%}
        {%- set hdiff_suffix = "" -%}
    {%- endif -%}

    {%- if datavault4dbt.is_list(multi_active_key) -%}
        {%- set multi_active_key = multi_active_key|join(", ") -%}
    {%- endif -%}

    {%- set standardise_prefix = "COALESCE(TO_HEX(" ~ hash_alg ~ "(TO_UTF8(ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(" ~ hdiff_prefix ~ "NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(" -%}
    
    {%- if not case_sensitive -%}
        {%- set standardise_prefix = standardise_prefix ~ "UPPER(CONCAT(" -%}
    {%- else -%}
        {%- set standardise_prefix = standardise_prefix ~ "CONCAT(" -%}
    {%- endif -%}

    {%- set standardise_suffix = "\n)), '\\n', '') \n, '\\t', '') \n, '\\v', '') \n, '\\r', '') AS VARCHAR), '[ALL_NULL]') " ~ hdiff_suffix ~ ")), '')))), CAST(" ~ zero_key ~ " AS VARCHAR))" -%}
    
    {%- if alias is not none -%}
        {%- set standardise_suffix = standardise_suffix ~ " AS " ~ alias -%}
    {%- endif -%}

    {%- do dict_result.update({"standardise_suffix": standardise_suffix, "standardise_prefix": standardise_prefix }) -%}
    {{ return(dict_result | tojson ) }}
{%- endmacro -%}