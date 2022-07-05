{%- macro parse_iso8601_date(column_name) -%}
    PARSE_DATE('%Y-%m-%d', {{ column_name }})
{%- endmacro -%}