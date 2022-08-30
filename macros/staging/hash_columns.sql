{%- macro hash_columns(columns=none) -%}

    {{- adapter.dispatch('hash_columns', 'dbtvault_scalefree')(columns=columns) -}}

{%- endmacro %}

{%- macro default__hash_columns(columns=none) -%}

{%- set hash = var('dbtvault_scalefree.hash', 'MD5') -%}
{#- Select hashing algorithm -#}
{%- set hash_alg, unknown_key, error_key = dbtvault_scalefree.hash_default_values(hash_function=hash) -%}

{%- if columns is mapping and columns is not none -%}

    {%- for col in columns -%}

        {% if columns[col] is mapping and columns[col].is_hashdiff -%}

            {{- dbtvault_scalefree.hash(columns=columns[col]['columns'],
                              alias=col,
                              is_hashdiff=columns[col]['is_hashdiff']) -}}

        {%- elif columns[col] is not mapping -%}
             {{- dbtvault_scalefree.hash(columns=columns[col], alias=col, is_hashdiff=false) }}

        {%- elif columns[col] is mapping and not columns[col].is_hashdiff -%}

            {%- if execute -%}
                {%- do exceptions.warn("[" ~ this ~ "] Warning: You provided a list of columns under a 'columns' key, but did not provide the 'is_hashdiff' flag. Use list syntax for PKs.") -%}
            {% endif %}

            {{- dbtvault_scalefree.hash(columns=columns[col]['columns'], alias=col) -}}

        {%- endif -%}

        {{- ",\n" if not loop.last -}}
    {%- endfor -%}

{%- endif %}
{%- endmacro -%}
