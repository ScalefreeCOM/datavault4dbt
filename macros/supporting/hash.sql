{%- macro hash(columns=none, alias=none, is_hashdiff=false) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'dbtvault_scalefree')(columns=columns, 
                                             alias=alias, 
                                             is_hashdiff=is_hashdiff) -}}

{%- endmacro %}

{%- macro default__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{%- set hashkey_input_case_sensitive = var('dbtvault_scalefree.hashkey_input_case_sensitive', FALSE) -%}
{%- set hashdiff_input_case_sensitive = var('dbtvault_scalefree.hashdiff_input_case_sensitive', TRUE) -%}

{#- Select hashing algorithm -#}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- set attribute_standardise = dbtvault_scalefree.attribute_standardise() %}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set columns = [columns] -%}
{%- endif -%}

{%- set all_null = [] -%}
{%- if is_hashdiff -%}
    {%- set standardise_prefix, standardise_suffix = dbtvault_scalefree.concattenated_standardise(case_sensitive=hashdiff_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key) -%}
{%- else -%}
    {%- set standardise_prefix, standardise_suffix = dbtvault_scalefree.concattenated_standardise(case_sensitive=hashkey_input_case_sensitive, hash_alg=hash_alg, alias=alias, zero_key=unknown_key) -%}
{%- endif -%}

{{ standardise_prefix }}

{%- for column in columns -%}

    {%- do all_null.append(null_placeholder_string) -%}

    {%- if '.' in column %}
        {% set column_str = column -%}
    {%- else -%}
        {%- set column_str = dbtvault_scalefree.as_constant(column) -%}
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


