{%- macro hash(columns=none, alias=none, is_hashdiff=false) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'dbtvault')(columns=columns, alias=alias, is_hashdiff=is_hashdiff) -}}

{%- endmacro %}

{%- macro default__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
{%- elif hash == 'SHA' -%}
    {%- set hash_alg = 'SHA2_BINARY' -%}
    {%- set hash_size = 32 -%}
{%- else -%}
    {%- set hash_alg = 'MD5' -%}
{%- endif -%}

{%- set standardise = "CONCAT( '\"' , REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), r'\\\\', '\\\\\\\\'), '\"', '\\\"'), '\"' )" %}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
    {{- "TO_HEX(({}({}))) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote), alias) | indent(4) -}}

{#- Else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}
    {# TODO: Update Hashdiff Function #}
    {%- if is_hashdiff -%}
        {{- "TO_HEX({}(CONCAT(".format(hash_alg) | indent(4) -}}
    {%- else -%}
        {{- "IFNULL(TO_HEX(LOWER({}(NULLIF(CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CONCAT(".format(hash_alg) | indent(4) -}}
    {%- endif -%}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- if '.' in column %}
            {% set column_str = column -%}
        {%- else -%}
            {%- set column_str = dbtvault.as_constant(column) -%}
        {%- endif -%}

        {{- "\nIFNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', column_str), null_placeholder_string) | indent(4) -}}
        {{- ",'{}',".format(concat_string) if not loop.last -}}

        {%- if loop.last -%}

            {% if is_hashdiff %}
                {{- "\n))) AS {}".format(alias) -}}
            {%- else -%}
                {{- "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '00000000000000000000000000000000') AS {}".format(all_null | join(""), alias) -}}
            {%- endif -%}
        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}