{% macro custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
{{ adapter.dispatch('custom_alter_relation_add_remove_columns', 'datavault4dbt')(relation=relation, add_columns=add_columns, remove_columns=remove_columns) }}
{% endmacro %}

{% macro default__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
{{ alter_relation_add_remove_columns(relation, add_columns, remove_columns) }}
{% endmacro %}

{% macro bigquery__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns)%}

    {% if add_columns %}

    {% set sql -%}
        ALTER TABLE {{ relation.render() }} 
            {% for column in add_columns %}
            ADD COLUMN IF NOT EXISTS {{ column.name }} {{ column.data_type }} {{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

    {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}
    
    {% do run_query(sql) %}
    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        ALTER TABLE {{ relation.render() }} 
            {% for column in remove_columns %}
            DROP COLUMN IF EXISTS {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

    {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}
    
    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}

{% macro fabric__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if add_columns %}

    {% set sql -%}
       ALTER TABLE {{ relation.render() }} ADD
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        ALTER TABLE {{ relation.render() }} DROP COLUMN
            {% for column in remove_columns %}
                {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}
    
    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}

{% macro synapse__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if add_columns %}

    {% set sql -%}
       ALTER TABLE {{ relation.render() }} ADD
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        ALTER TABLE {{ relation.render() }} DROP COLUMN
            {% for column in remove_columns %}
                {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}
    
    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}

{% macro databricks__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if add_columns or remove_columns %}

    {# 1. Handle ADD COLUMNS (Can be done in one batch) #}
    {% if add_columns %}
        {% set add_sql -%}
            ALTER TABLE {{ relation.render() }} ADD COLUMNS (
              {% for column in add_columns %}
                {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
              {% endfor %}
            )
        {%- endset %}
        {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('Executing ADD COLUMNS on Databricks: ' ~ add_sql, false) }}{% endif %}
        {% do run_query(add_sql) %}
    {% endif %}

    {# 2. Handle REMOVE COLUMNS (Execute sequentially to avoid Syntax Errors) #}
    {% if remove_columns %}
        {% for column in remove_columns %}
            {% set remove_sql -%}
                ALTER TABLE {{ relation.render() }} DROP COLUMN IF EXISTS {{ column.name }}
            {%- endset %}
            {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('Executing DROP COLUMN on Databricks: ' ~ remove_sql, false) }}{% endif %}
            {% do run_query(remove_sql) %}
        {% endfor %}
    {% endif %}

    {% endif %}

{% endmacro %}

{% macro snowflake__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

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

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}

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

{% macro redshift__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {# In redshift, distkey and sortkey columns cannot be dropped. In remove_columns an alternative column has to be provided. #}

    {% if add_columns %}

    {% set sql -%}
        {% for column in add_columns %}
            ALTER TABLE {{ relation.render() }} ADD COLUMN {{ column.name }} {{ column.data_type }};
        {% endfor %}
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

        {%- if execute -%}
            
            {# Get sorkey order #}
            {%- set sortkey_query -%}

                SET search_path = public, {{ relation.schema }};

                SELECT 
                    "column"
                    ,sortkey
                FROM pg_table_def
                WHERE schemaname = '{{ relation.schema }}'
                    AND tablename = '{{ relation.identifier }}'
                    AND sortkey <> 0
                    ORDER BY sortkey

            {%- endset -%}
            
            {%- set sortkey_results = run_query(sortkey_query) -%}
            {%- set initial_sort_keys_dict = {} -%}

            {%- if sortkey_results is not none and sortkey_results | length > 0 -%}
                {% for row in sortkey_results.rows %}
                    {%- do initial_sort_keys_dict.update({row[0] | lower: row[1]}) -%}
                {% endfor %}
            {%- endif -%}
            {%- set ns = namespace(drop_statements = "", final_sort_keys_dict = initial_sort_keys_dict.copy(), sortkey_was_modified=false) -%}

            
            {%- for item in remove_columns -%}

                {%- set col_to_drop = item['name'] | lower -%}
                {%- set new_distkey_column = item['new_name'] | lower if item['new_name'] is not none else none -%}

               {# pop old column from sortkey order and put in new column #}
                {%- if col_to_drop in ns.final_sort_keys_dict -%}
                    {%- set ns.sortkey_was_modified = true -%}
                    {%- set sortkey_position = ns.final_sort_keys_dict[col_to_drop] -%}
                    
                    {%- do ns.final_sort_keys_dict.pop(col_to_drop) -%}
                    
                    {%- if new_distkey_column and new_distkey_column | length > 0 -%}
                        {%- do ns.final_sort_keys_dict.update({new_distkey_column: sortkey_position}) -%}
                    {%- endif -%}
                {%- endif -%}

                {# Check if column is distkey #}
                {%- set distkey_check_query -%}

                    SET search_path = public, '{{ relation.schema }}';

                    SELECT
                        "column"
                    FROM
                        pg_table_def
                    WHERE 
                        tablename = '{{ relation.identifier }}'
                        AND distkey = true
                        AND "column" = '{{ col_to_drop }}'

                {%- endset -%}

                {%- set distkey_result = run_query(distkey_check_query) -%}
                {%- set is_distkey = distkey_result | length > 0 -%}
                
                {# Change distkey to new column. #}
                {%- if is_distkey -%}
                    
                    {%- if new_distkey_column -%}
                        
                        {%- set alter_sql = "ALTER TABLE " ~ relation ~ " ALTER DISTSTYLE KEY DISTKEY " ~ new_distkey_column -%}
                        {% if var('datavault4dbt.show_debug_logs', false) %}{{ log("Column " ~ col_to_drop ~ " is the DISTKEY. Changing DISTKEY to: " ~ new_distkey_column, false) }}{% endif %}

                    {%- else -%}
                        
                        {# {%- do exceptions.raise_compiler_error(
                            "Cannot drop column '" ~ col_to_drop ~ "' on table " ~ relation ~ 
                            " because it is the DISTKEY and no 'new_name' replacement column was provided. " ~
                            "Provide a valid 'new_name' column or change DISTSTYLE to EVEN."
                        ) -%} #}
                        {%- set alter_sql = "ALTER TABLE " ~ relation ~ " ALTER DISTSTYLE EVEN" -%}
                        {{ log("Column " ~ col_to_drop ~ " is the DISTKEY, but no replacement was provided. Changing DISTSTYLE to EVEN.", true) }}
                    
                    {%- endif -%}

                    {%- do run_query(alter_sql) -%}

                {%- endif -%}


            {%- set drop_sql = "ALTER TABLE " ~ relation ~ " DROP COLUMN " ~ col_to_drop ~ ";\n" -%}
            {%- set ns.drop_statements = ns.drop_statements + drop_sql -%}
                
            {%- endfor -%}
            {% if var('datavault4dbt.show_debug_logs', false) %}{{ log(ns.drop_statements,false)}}{% endif %}

            {%- if ns.sortkey_was_modified -%}

                {%- set sorted_columns = ns.final_sort_keys_dict.items() | sort(attribute='1') -%}
                {%- set final_sort_keys_list = sorted_columns | map(attribute='0') | list -%}

                {%- if final_sort_keys_list | length > 0 -%}
                    {%- set sortkey_list_str = final_sort_keys_list | join(', ') -%}
                    {%- set alter_sortkey_sql = "ALTER TABLE " ~ relation ~ " ALTER SORTKEY (" ~ sortkey_list_str ~ ")" -%}
                    {% if var('datavault4dbt.show_debug_logs', false) %}{{ log("Finalizing SORTKEY change. New list: (" ~ sortkey_list_str ~ ")", false) }}{% endif %}
                {%- else -%}
                    {# {%- do exceptions.raise_compiler_error(
                        "Cannot drop columns on table " ~ relation ~ 
                        " because it has defined SORTKEYs and no 'new_name' replacement was provided for all columns. " ~
                        "Provide valid 'new_name' columns or set SORTKEY to NONE."
                    ) -%} #}
                    {%- set alter_sortkey_sql = "ALTER TABLE " ~ relation ~ " ALTER SORTKEY NONE" -%}
                    {{ log("Finalizing SORTKEY change. Setting SORTKEY to NONE. Original order was: " ~
                        initial_sort_keys_dict.items() | sort(attribute='1')
                        , true) }}

                {%- endif -%}
                {% if var('datavault4dbt.show_debug_logs', false) %}{{ log(alter_sortkey_sql, false)}}{% endif %}
                {%- do run_query(alter_sortkey_sql) -%}

            {%- endif -%}
            
            {% if var('datavault4dbt.show_debug_logs', false) %}{{ log("Executing DROP COLUMN: " ~ ns.drop_statements, false) }}{% endif %}
            
            {%- do run_query(ns.drop_statements) -%}


        {%- endif -%}


    {% endif %}

{% endmacro %}

{% macro postgres__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}


    {% if add_columns %}

    {% set sql -%}
        {% for column in add_columns %}
            ALTER TABLE {{ relation.render() }} ADD COLUMN {{ column.name }} {{ column.data_type }};
        {% endfor %}
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}
    
        {%- set ns = namespace(drop_statements = "") -%}

        {% for column in remove_columns %}

            {%- set drop_sql = "ALTER TABLE " ~ relation ~ " DROP COLUMN " ~ column.name ~ ";\n" -%}
            {%- set ns.drop_statements = ns.drop_statements + drop_sql -%}
                
        {%- endfor -%}
        
        {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ ns.drop_statements, false)}}{% endif %}

    {% do run_query(ns.drop_statements) %}

    {% endif %}

{% endmacro %}

{% macro exasol__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {# Handle adding columns #}
    {% if add_columns %}
        {% for column in add_columns %}
            {% set sql -%}
                ALTER TABLE {{ relation.render() }} 
                ADD COLUMN {{ column.name }} {{ column.data_type }};
            {%- endset -%}
            
            {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql (add column): ' ~ sql, false)}}{% endif %}
            
            {% do run_query(sql) %}
        {% endfor %}
    {% endif %}
    
    {# Handle removing columns #}
    {% if remove_columns %}
        {% for column in remove_columns %}
            {% set sql -%}
                ALTER TABLE {{ relation.render() }} 
                DROP COLUMN {{ column.name }}
            {%- endset -%}
            
            {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql (drop column): ' ~ sql, false)}}{% endif %}
            
            {% do run_query(sql) %}
        {% endfor %}
    {% endif %}

{% endmacro %}

{% macro oracle__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if add_columns %}

    {% set sql -%}
       ALTER TABLE {{ relation.render() }} ADD (
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
       )
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}

    {% set sql -%}
        ALTER TABLE {{ relation.render() }} DROP (
            {% for column in remove_columns %}
                {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
        )
    {%- endset -%}

     {% if var('datavault4dbt.show_debug_logs', false) %}{{ log('alter sql: ' ~ sql, false)}}{% endif %}
    
    {% do run_query(sql) %}

    {% endif %}

{% endmacro %}

{% macro trino__custom_alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {% if add_columns %}

    {# ADD COLUMN is supported by Trino. #}
    {% set sql -%}
        ALTER TABLE {{ relation.render() }} ADD COLUMN
            {% for column in add_columns %}
                {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
            {% endfor %}
    {%- endset -%}

    {{ log('alter sql: ' ~ sql, false)}}

    {% do run_query(sql) %}
    {% endif %}

    {% if remove_columns %}

    {# Trino memory connector does not support DROP COLUMN.
       Use CTAS + DROP + RENAME to achieve the equivalent result. #}
    {% set cols_to_drop = [] %}
    {% for col in remove_columns %}
        {% do cols_to_drop.append(col.name.lower()) %}
    {% endfor %}

    {% set existing_columns = adapter.get_columns_in_relation(relation) %}
    {% set keep_cols = [] %}
    {% for col in existing_columns %}
        {% if col.name.lower() not in cols_to_drop %}
            {% do keep_cols.append(col.name) %}
        {% endif %}
    {% endfor %}

    {% set temp_identifier = relation.identifier ~ '_drop_col_tmp' %}
    {% set temp_relation = api.Relation.create(
        database=relation.database,
        schema=relation.schema,
        identifier=temp_identifier
    ) %}

    {# Clean up any orphaned temp table from a previous failed run. #}
    {% set drop_tmp_sql %}DROP TABLE IF EXISTS {{ temp_relation }}{% endset %}
    {% do run_query(drop_tmp_sql) %}

    {% set ctas_sql %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT {{ keep_cols | join(', ') }}
    FROM {{ relation }}
    {% endset %}
    {{ log('CTAS drop-column sql: ' ~ ctas_sql, false) }}
    {% do run_query(ctas_sql) %}

    {% set drop_sql %}DROP TABLE {{ relation }}{% endset %}
    {% do run_query(drop_sql) %}

    {% set rename_sql %}ALTER TABLE {{ temp_relation }} RENAME TO {{ relation.identifier }}{% endset %}
    {% do run_query(rename_sql) %}

    {% endif %}

{% endmacro %}