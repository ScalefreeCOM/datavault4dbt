{% macro fabric_delete_table(table_name) %}

    {% set table_relation = ref(table_name) %}

    {% set drop_sql %}
        DROP TABLE {{ table_relation }}
    {% endset %}
        
    {% do run_query(drop_sql) %}  

{% endmacro %}