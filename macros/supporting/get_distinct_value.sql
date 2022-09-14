{% macro get_distinct_value(source_relation, column_name, exclude_values=none) %}

{{return(adapter.dispatch('get_distinct_value', 'datavault4dbt')(source_relation= source_relation, 
                                                            column_name= column_name,
                                                            exclude_values=exclude_values) )}}

{% endmacro %}
{%- macro default__get_distinct_value(source_relation, column_name, exclude_values) -%}

{% set query %}
    SELECT DISTINCT {{ column_name }}
    from {{ source_relation }}
    {% if exclude_values is not none %}
        where {{ column_name }} not in ( {%- for value in exclude_values -%} 
                                            '{{ value }}'
                                            {%- if not loop.last -%}, {%- endif -%}
                                        {%- endfor -%} )
    {% endif %}
    LIMIT 1
{% endset %}

{% set results = run_query(query) %}

{% if execute %}

    {% set result_value = results.columns[0].values()[0] %}
{% else %}
    {% set result_value = "" %}
{% endif %}

{{ return(result_value) }}

{% endmacro %}