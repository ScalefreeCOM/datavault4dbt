{%- macro hash(columns=none, alias=none, is_hashdiff=false) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'dbtvault')(columns=columns, 
                                             alias=alias, 
                                             is_hashdiff=is_hashdiff) -}}

{%- endmacro %}

{%- macro default__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
    {%- set zero_key = '00000000000000000000000000000000' -%}
{%- elif hash == 'SHA' or hash == 'SHA1' -%}
    {%- set hash_alg = 'SHA1' -%}
    {%- set zero_key = '0000000000000000000000000000000000000000' -%}
{%- elif hash == 'SHA2' or hash == 'SHA256' -%}
    {%- set hash_alg = 'SHA256' -%}
    {%- set zero_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
{%- endif -%}

{%- set attribute_standardise = attribute_standardise() %}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set columns = [columns] -%}
{%- endif -%}

{%- set all_null = [] -%}
{%- if is_hashdiff -%}
    {%- set standardise_prefix, standardise_suffix = concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=zero_key) -%}
{%- else -%}
    {%- set standardise_prefix, standardise_suffix = concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=zero_key) -%}
{%- endif -%}

{{ standardise_prefix }}

{%- for column in columns -%}

    {%- do all_null.append(null_placeholder_string) -%}

    {%- if '.' in column %}
        {% set column_str = column -%}
    {%- else -%}
        {%- set column_str = dbtvault.as_constant(column) -%}
    {%- endif -%}

    {{- "\nIFNULL(({}), '{}')".format(attribute_standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote) | replace('[NULL_PLACEHOLDER_STRING]', null_placeholder_string), null_placeholder_string) | indent(4) -}}
    {{- ",'{}',".format(concat_string) if not loop.last -}}

    {%- if loop.last -%}

        {{ standardise_suffix | indent(4) }}

    {%- else -%}

        {%- do all_null.append(concat_string) -%}

    {%- endif -%}

{%- endfor -%}


{%- endmacro -%}


