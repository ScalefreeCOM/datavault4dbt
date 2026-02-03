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

{% macro synapse__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

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

{% macro databricks__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

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
        {{ log('Executing ADD COLUMNS on Databricks: ' ~ add_sql, true) }}
        {% do run_query(add_sql) %}
    {% endif %}

    {# 2. Handle REMOVE COLUMNS (Execute sequentially to avoid Syntax Errors) #}
    {% if remove_columns %}
        {% for column in remove_columns %}
            {% set remove_sql -%}
                ALTER TABLE {{ relation.render() }} DROP COLUMN IF EXISTS {{ column.name }}
            {%- endset %}
            {{ log('Executing DROP COLUMN on Databricks: ' ~ remove_sql, true) }}
            {% do run_query(remove_sql) %}
        {% endfor %}
    {% endif %}

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

{% macro redshift__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {# In redshift, distkey and sortkey columns cannot be dropped. In remove_columns an alternative column has to be provided. #}

    {% if add_columns %}

    {% set sql -%}
        {% for column in add_columns %}
            ALTER TABLE {{ relation.render() }} ADD COLUMN {{ column.name }} {{ column.data_type }};
        {% endfor %}
    {%- endset -%}

     {{ log('alter sql: ' ~ sql, false)}}

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
                        {{ log("Column " ~ col_to_drop ~ " is the DISTKEY. Changing DISTKEY to: " ~ new_distkey_column, false) }}

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
            {{ log(ns.drop_statements,false)}}

            {%- if ns.sortkey_was_modified -%}

                {%- set sorted_columns = ns.final_sort_keys_dict.items() | sort(attribute='1') -%}
                {%- set final_sort_keys_list = sorted_columns | map(attribute='0') | list -%}

                {%- if final_sort_keys_list | length > 0 -%}
                    {%- set sortkey_list_str = final_sort_keys_list | join(', ') -%}
                    {%- set alter_sortkey_sql = "ALTER TABLE " ~ relation ~ " ALTER SORTKEY (" ~ sortkey_list_str ~ ")" -%}
                    {{ log("Finalizing SORTKEY change. New list: (" ~ sortkey_list_str ~ ")", false) }}
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
                {{ log(alter_sortkey_sql, false)}}
                {%- do run_query(alter_sortkey_sql) -%}

            {%- endif -%}
            
            {{ log("Executing DROP COLUMN: " ~ ns.drop_statements, false) }}
            
            {%- do run_query(ns.drop_statements) -%}


        {%- endif -%}


    {% endif %}

{% endmacro %}

{% macro postgres__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}


    {% if add_columns %}

    {% set sql -%}
        {% for column in add_columns %}
            ALTER TABLE {{ relation.render() }} ADD COLUMN {{ column.name }} {{ column.data_type }};
        {% endfor %}
    {%- endset -%}

     {{ log('alter sql: ' ~ sql, false)}}

    {% do run_query(sql) %}

    {% endif %}

    {% if remove_columns %}
    
        {%- set ns = namespace(drop_statements = "") -%}

        {% for column in remove_columns %}

            {%- set drop_sql = "ALTER TABLE " ~ relation ~ " DROP COLUMN " ~ column.name ~ ";\n" -%}
            {%- set ns.drop_statements = ns.drop_statements + drop_sql -%}
                
        {%- endfor -%}
        
        {{ log('alter sql: ' ~ ns.drop_statements, false)}}

    {% do run_query(ns.drop_statements) %}

    {% endif %}

{% endmacro %}

{% macro exasol__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

    {# Handle adding columns #}
    {% if add_columns %}
        {% for column in add_columns %}
            {% set sql -%}
                ALTER TABLE {{ relation.render() }} 
                ADD COLUMN {{ column.name }} {{ column.data_type }};
            {%- endset -%}
            
            {{ log('alter sql (add column): ' ~ sql, false)}}
            
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
            
            {{ log('alter sql (drop column): ' ~ sql, false)}}
            
            {% do run_query(sql) %}
        {% endfor %}
    {% endif %}

{% endmacro %}