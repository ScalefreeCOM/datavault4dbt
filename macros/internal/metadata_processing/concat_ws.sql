{%- macro concat_ws(string_list, separator="||") -%}

    {{- adapter.dispatch('concat_ws', 'datavault4dbt')(string_list=string_list, separator=separator) -}}

{%- endmacro %}

{%- macro default__concat_ws(string_list, separator="||") -%}

    {{- 'CONCAT(' -}}
    {%- for str in string_list -%}
        {{- "{}".format(str) -}}
        {{- ",'{}',".format(separator) if not loop.last -}}
    {%- endfor -%}
    {{- '\n)' -}}

{%- endmacro -%}

{%- macro exasol__concat_ws(string_list, separator="||") -%}

    {{- 'CONCAT(' -}}
    {%- for str in string_list -%}
        {{- "{}".format(str) -}}
        {{- ",'{}',".format(separator) if not loop.last -}}
    {%- endfor -%}
    {{- ')' -}}

{%- endmacro -%}

{%- macro redshift__concat_ws(string_list, separator="|") -%}

    {%- for str in string_list -%}
        {{- "{}".format(str) -}}
        {{- "|| '{}' ||".format(separator) if not loop.last -}}
    {%- endfor -%}

{%- endmacro -%}

{%- macro oracle__concat_ws(string_list, separator="|") -%}

    {%- for str in string_list -%}
        {{- "{}".format(str) -}}
        {{- "|| '{}' ||".format(separator) if not loop.last -}}
    {%- endfor -%}

{%- endmacro -%}

{%- macro trino__concat_ws(string_list, separator="||") -%}

    {%- if string_list | length == 1 -%}
        {{- "CAST({} AS VARCHAR)".format(string_list[0]) -}}
    {%- else -%}
        {{- 'CONCAT(' -}}
        {%- for str in string_list -%}
            {{- "CAST({} AS VARCHAR)".format(str) -}}
            {{- ",'{}',".format(separator) if not loop.last -}}
        {%- endfor -%}
        {{- '\n)' -}}
    {%- endif -%}

{%- endmacro -%}