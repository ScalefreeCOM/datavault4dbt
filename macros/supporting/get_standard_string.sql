{%- macro get_standard_string(string_list) -%}

RTRIM(CONCAT(
    {%- for column in string_list -%}
        IFNULL(TRIM(CAST({{ column }} AS STRING)), '^^'), '||'
        {%- if not loop.last -%}, {% endif %}
    {% endfor %}
))
{%- endmacro -%}