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

{%- endmacro -%}

{%- macro redshift__concat_ws(string_list, separator="|") -%}

    {%- for str in string_list -%}
        {{- "{}".format(str) -}}
        {{- "|| '{}' ||".format(separator) if not loop.last -}}
    {%- endfor -%}

{%- endmacro -%}