{#
    Usage example:
    dbt run-operation rehash_single_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=(satellite)) %}

    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey %}
    {% set new_hashdiff_name = hashdiff %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {% if datavault4dbt.is_something(business_keys) %}
        {# Ensuring business_keys is a list. #}
        {% if business_keys is iterable and business_keys is not string %}
            {% set business_key_list = business_keys %}
        {% else %}
            {% set business_key_list = [business_keys] %}
        {% endif %}

        {# Adding prefixes to column names for proper selection. #}
        {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='src').split(',') %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}

        {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys, new_hashdiff_name: prefixed_payload} %}
        
    {% else %}

        {# Adding prefixes to column names for proper selection. #}
        {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='src').split(',') %}

        {% set hash_config_dict = {new_hashdiff_name: prefixed_payload} %}

    {% endif %}

    {# generating the CREATE statement that populates the new columns. #}
    {% set create_sql = datavault4dbt.satellite_update_statement(satellite_relation=satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                new_hashdiff_name=new_hashdiff_name,
                                                hashkey=hashkey, 
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...', output_logs) }}
    {{ '/* CREATE STATEMENT FOR ' ~ satellite ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'},
        {"name": hashdiff + '_deprecated'}
    ]%}
    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
            ALTER TABLE {{ satellite_relation }} 
                RENAME COLUMN {{ hashkey }} TO {{ hashkey }}_deprecated;
            ALTER TABLE {{ satellite_relation }} 
                RENAME COLUMN {{ hashdiff }} TO {{ hashdiff }}_deprecated;
        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        {% if drop_old_values %}
            {% set old_table_name = satellite_relation %}
            {% set new_table_name = satellite_relation.database ~ '.' ~ satellite_relation.schema ~ '.' ~ satellite_relation.identifier ~ '_rehashed' %}

            {{ log('Dropping old table: ' ~ old_table_name, output_logs) }}
            {% do run_query(bigquery__drop_table(old_table_name)) %}

            {% set rename_sql = bigquery__get_rename_table_sql(new_table_name, satellite_relation.identifier) %}
            {{ log('Renaming rehashed Sat to original Sat name: ' ~ rename_sql, output_logs) }}
            {% do run_query(rename_sql) %}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro bigquery__satellite_update_statement(satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, ldts_col, hash_config_dict, parent_relation) %}

    {% set ns = namespace(parent_already_rehashed=false) %}

    {#
        If parent entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
        hashkey column needs to be used for joining, and the regular hashkey should be selected. 

        Otherwise, the regular hashkey should be used for joining. 
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {{ log('parent column names: ' ~ all_parent_columns, output_logs) }}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('parent_already hashed set to true for ' ~ satellite_relation.name, true) }}
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
    {% set all_columns = adapter.get_columns_in_relation(satellite_relation) %}

    {# Filter out the excluded columns by name #}
    {% set filtered_columns = [] %}
    {% for col in all_columns %}
    {% if col.name not in exclude_columns %}
        {% do filtered_columns.append(col) %}
    {% endif %}
    {% endfor %}

    {# Extracting Businesskey and HD columns, since they dont get passed to the create macro#}
    {% set hash_value_list = [] %}
    {% for keylist in hash_config_dict.values() %}
        {% for col in keylist %}
            {% do hash_value_list.append(col) %}
        {% endfor %}
    {% endfor %}

    {# Extract only the column names #}
    {% set selected_column_names = filtered_columns | map(attribute='name') | list %}
    {% set select_clause = selected_column_names | join(', ') %}
    {{ log('SELECT clause: ' ~ select_clause, output_logs) }}

    {% set rsrc_alias = 'rsrc' %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', '(error)') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', '(unknown)') %}

    {{ log('hash_config_dict' ~ hash_config_dict, true) }}

    {% set create_sql %}
    CREATE OR REPLACE TABLE {{ satellite_relation.database }}.{{ satellite_relation.schema }}.{{ satellite_relation.identifier ~ '_rehashed' }} AS
        
        WITH calculate_hd_correctly AS (
            SELECT
                src.{{ hashkey }} AS original_hashkey,
                src.{{ ldts_col }} AS original_ldts,
                src.{{ rsrc_alias }} AS original_rsrc,
                {% if new_hashkey_name not in hash_config_dict.keys() %}
                    {# If Business Keys are not defined for parent entity, use new hashkey already existing in parent entitiy. #}
                    parent.{{ select_hashkey_col }} as {{ new_hashkey_name }},
                {% endif %} 
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }},
            FROM {{ satellite_relation }} src
            JOIN {{ parent_relation }} parent
            ON src.{{ hashkey }} = parent.{{ hashkey }}
            WHERE src.{{ rsrc_alias }} NOT IN ('ERROR', 'SYSTEM')
            GROUP BY src.{{ hashkey }}, src.{{ ldts_col }}, src.{{ rsrc_alias }}, {{ datavault4dbt.print_list(hash_value_list)}}
        )    
        SELECT 
            hd.{{ hashkey }},
            hd.{{ new_hashdiff_name }},
            sat.{{ select_clause }}
        FROM calculate_hd_correctly hd
        LEFT JOIN {{ satellite_relation }} sat
        ON hd.{{ hashkey }} = sat.{{ hashkey }}

        UNION ALL

        SELECT 
            * 
        FROM {{ satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('ERROR', 'SYSTEM')

    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}