{#
    Usage example:
    dbt run-operation rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set ma_satellite_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=(ma_satellite)) %}

    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'LOAD_DATE') %}

    {% set new_hashkey_name = hashkey %}
    {% set new_hashdiff_name = hashdiff %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Ensuring ma_keys is a list. #}
    {% if ma_keys is iterable and ma_keys is not string %}
        {% set ma_keys = ma_keys %}
    {% else %}
        {% set ma_keys = [ma_keys] %}
    {% endif %}

    {% set is_hashdiff = true %}

    {# Adding prefixes to column names for proper selection. #}
    {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
    {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}

    {% set hash_config_dict = {
            new_hashkey_name: prefixed_business_keys, 
            new_hashdiff_name: {
                "is_hashdiff": is_hashdiff, 
                "columns": payload
                }
            } %}


    {# generating the CREATE statement that populates the new columns. #}
    {% set create_sql = datavault4dbt.ma_satellite_update_statement(ma_satellite_relation=ma_satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                new_hashdiff_name=new_hashdiff_name,
                                                hashkey=hashkey, 
                                                business_key_list=business_key_list,
                                                ma_keys=ma_keys,
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...' ~ create_sql, true) }}
    {{ '/* CREATE STATEMENT FOR ' ~ ma_satellite ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Renaming existing hash columns...', output_logs) }}

        {% set overwrite_sql %}
            ALTER TABLE {{ ma_satellite_relation }} 
                RENAME COLUMN {{ hashkey }} TO {{ hashkey }}_deprecated;
            ALTER TABLE {{ ma_satellite_relation }} 
                RENAME COLUMN {{ hashdiff }} TO {{ hashdiff }}_deprecated;
        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        
        {% if drop_old_values %}
            {# Drop old Hub table and rename _rehashed Hub table to original Hub name. #}
            {% set old_table_name = ma_satellite_relation %}
            {% set new_table_name = ma_satellite_relation.database ~ '.' ~ ma_satellite_relation.schema ~ '.' ~ ma_satellite_relation.identifier ~ '_rehashed' %}

            {{ log('Dropping old table: ' ~ old_table_name, output_logs) }}
            {% do run_query(bigquery__drop_table(old_table_name)) %}

            {% set rename_sql = bigquery__get_rename_table_sql(new_table_name, ma_satellite_relation.identifier) %}
            {{ log('Renaming rehashed Hub to original Hub name: ' ~ rename_sql, output_logs) }}
            {% do run_query(rename_sql) %}
        {% endif %}

    {% endif %}


{% endmacro %}


{% macro bigquery__ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}


    {% set ns = namespace(update_where_condition='', parent_already_rehashed=false) %}

    {% set prefixed_hashkey = 'sat.' ~ hashkey %}

    {#
        If parent entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
        hashkey column needs to be used for joining, and the regular hashkey should be selected. 

        Otherwise, the regular hashkey should be used for joining. 
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('parent_already hashed set to true for ' ~ ma_satellite_relation.name, true) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}

    {% set exclude_columns = [new_hashkey_name, new_hashdiff_name] %}
    {% set all_columns = adapter.get_columns_in_relation(ma_satellite_relation) %}

    {# Filter out the excluded columns by name #}
    {% set filtered_columns = [] %}
    {% for col in all_columns %}
    {% if col.name not in exclude_columns %}
        {% do filtered_columns.append(col) %}
    {% endif %}
    {% endfor %}

    {# Extract only the column names #}
    {% set selected_column_names = filtered_columns | map(attribute='name') | list %}
    {% set select_clause = selected_column_names | join(', ') %}
    {{ log('SELECT clause: ' ~ select_clause, output_logs) }}

    
    {% set rsrc_alias = 'RECORD_SOURCE' %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', '(error)') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', '(unknown)') %}

    {# 
        This does finally kinda work, but only if the payload and ma_keys columns are not in the parent, which unfortunattly is the case for the automated test repo... (here: stage_03)
    #}
    {% set create_sql %}
    CREATE  OR REPLACE TABLE {{ ma_satellite_relation.database }}.{{ ma_satellite_relation.schema }}.{{ma_satellite_relation.identifier ~ '_rehashed'}} AS 
    
        WITH calculate_hd_correctly AS (
            SELECT
                src.{{ hashkey }} AS original_hashkey,
                src.{{ ldts_col }} AS original_ldts,
                src.{{ rsrc_alias }} AS original_rsrc,
                {% if new_hashkey_name not in hash_config_dict.keys() %}
                    {# If Business Keys are not defined for parent entity, use new hashkey already existing in parent entitiy. #}
                    parent.{{ select_hashkey_col }} as {{ new_hashkey_name }},
                {% endif %} 
                {{ datavault4dbt.hash_columns(columns=hash_config_dict, main_hashkey_column=prefixed_hashkey, multi_active_key=ma_keys) }},
            FROM {{ ma_satellite_relation }} src
            JOIN {{ parent_relation }} parent
            ON src.{{ hashkey }} = parent.{{ hashkey }}
            WHERE src.{{ rsrc_alias }} NOT IN ('(error)', '(unknown)')
            GROUP BY src.{{ hashkey }}, src.{{ ldts_col }}, src.{{ rsrc_alias }}, {{ datavault4dbt.print_list(business_key_list, src_alias='parent') }}      
        )
        SELECT 
            hd.{{ hashkey }},
            hd.{{ new_hashdiff_name }},
            sat.{{ select_clause }}
        FROM calculate_hd_correctly hd
        LEFT JOIN {{ ma_satellite_relation }} sat
        ON hd.{{ hashkey }} = sat.{{ hashkey }}

        UNION ALL

        SELECT 
            * 
        FROM {{ ma_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('(error)', '(unknown)')

            
    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}
