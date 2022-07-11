
{%- macro hash(columns=none, alias=none, is_hashdiff=false) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'dbtvault-scalefree')(columns=columns, alias=alias, is_hashdiff=is_hashdiff) -}}

{%- endmacro %}

{%- macro bigquery__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set quote = var('quote', '"') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
    {%- set hash_size_char = 32 -%}
    {%- set zero_key = '00000000000000000000000000000000' -%}
{%- elif hash == 'SHA' or hash == 'SHA1' -%}
    {%- set hash_alg = 'SHA1' -%}
    {%- set hash_size_char = 40 -%}
    {%- set zero_key = '0000000000000000000000000000000000000000' -%}
{%- elif hash == 'SHA2' or hash == 'SHA256' -%}
    {%- set hash_alg = 'SHA256' -%}
    {%- set hash_size_char = 64 -%}
    {%- set zero_key = '0000000000000000000000000000000000000000000000000000000000000000' -%}
{%- endif -%}

{%- set standardise = "CONCAT( '\"' , REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST([EXPRESSION] AS STRING)), r'\\\\', '\\\\\\\\'), '\"', '\\\"'), '\"' )" %}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
    {{- "TO_HEX(({}({}))) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', column_str) | replace('[QUOTE]', quote), alias) | indent(4) -}}

{#- Else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}
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

        {{- "\nREGEXP_REPLACE(IFNULL({}, '{}'), '{}', '--')".format(standardise | replace('[EXPRESSION]', column_str), null_placeholder_string, null_placeholder_string) | indent(4) -}}
        {{- ",'{}',".format(concat_string) if not loop.last -}}

        {%- if loop.last -%}

            {% if is_hashdiff %}
                {{- "\n))) AS {}".format(alias) -}}
            {%- else -%}
                {{- "\n), r'\\n', '') \n, r'\\t', '') \n, r'\\v', '') \n, r'\\r', '') AS STRING), '{}')))), '{}') AS {}".format(all_null | join(""), zero_key, alias) -}}
            {%- endif -%}
        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}