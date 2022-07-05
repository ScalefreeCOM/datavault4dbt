{%- macro parse_iso8601(column_name) -%}
    PARSE_TIMESTAMP('%Y-%m-%dT%H-%M-%S', {{ column_name }})
{%- endmacro -%}