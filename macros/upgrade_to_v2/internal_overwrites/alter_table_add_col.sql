{% macro alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
{{ adapter.dispatch('alter_relation_add_remove_columns', 'datavault4dbt')(relation=relation, add_columns=add_columns, remove_columns=remove_columns) }}
{% endmacro %}

{% macro fabric__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if add_columns %}

    {% set sql -%}
       ALTER TABLE {{ relation.render() }} ADD
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
    {%- endset -%}

     {{ log('alter sql: ' ~ sql, false)}}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        ALTER TABLE {{ relation.render() }} DROP COLUMN
            {% for column in remove_columns %}
                {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

     {{ log('alter sql: ' ~ sql, false)}}
    
    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}

{% macro snowflake__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if relation.is_dynamic_table -%}
        {% set relation_type = "dynamic table" %}
    {% else -%}
        {% set relation_type = "Table" %}
    {% endif %}

    {% if add_columns %}

    {% set sql -%}
       alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} add column
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
    {%- endset -%}

     {{ log('alter sql: ' ~ sql, false)}}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        alter {{ relation.get_ddl_prefix_for_alter() }} {{ relation_type }} {{ relation.render() }} drop column
            {% for column in remove_columns %}
                {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}
