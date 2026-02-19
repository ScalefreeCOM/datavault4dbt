{#
    Usage example:
    dbt run-operation rehash_single_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(satellite) %}

    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% if overwrite_hash_values %}
        {% set new_hashkey_name = hashkey %}
        {% set new_hashdiff_name = hashdiff %}
    {% else %}
        {% set new_hashkey_name = hashkey + '_new' %}
        {% set new_hashdiff_name = hashdiff + '_new' %}
    {% endif %}

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

        {% set hash_config_dict = {
                new_hashkey_name: prefixed_business_keys, 
                new_hashdiff_name: {
                    "is_hashdiff": true,
                    "columns": prefixed_payload
                    } 
                } %}
        
    {% else %}

        {# Adding prefixes to column names for proper selection. #}
        {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='src').split(',') %}

        {% set hash_config_dict = {
                new_hashdiff_name: {
                        "is_hashdiff": true,
                        "columns": prefixed_payload
                        } 
                } %}

    {% endif %}

    {% set rename_sql = get_rename_table_sql(satellite_relation, satellite_relation.identifier ~ '_deprecated') %}
    {% do run_query(rename_sql) %}

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
        
    {% if drop_old_values %}
        {% set old_table_relation = make_temp_relation(satellite_relation,suffix='_deprecated') %}

        {{ log('Dropping old table: ' ~ old_table_relation, output_logs) }}
        {% do run_query(drop_table(old_table_relation)) %}
        {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro bigquery__satellite_update_statement(satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, ldts_col, hash_config_dict, parent_relation) %}

    {% set ns = namespace(parent_already_rehashed=false) %}
    
    {% set old_hashkey_name = hashkey + '_deprecated' %}
    {% set old_table_relation = make_temp_relation(satellite_relation,suffix='_deprecated') %}
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
            {{ log('parent_already hashed set to true for ' ~ satellite_relation.name, false) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}


    {# Extract only the column names #}
    {% set all_columns = adapter.get_columns_in_relation(old_table_relation) | map(attribute='name') | list %}

    {% if new_hashdiff_name not in all_columns %}
        {% set hashdiff_name = new_hashdiff_name | replace('_new', '') %}

        {% if hashdiff_name in all_columns %}
            {% set old_hashdiff_name = hashdiff_name + '_deprecated'%}
            {% set exclude_columns = [new_hashkey_name, hashdiff_name] %}
        {% endif %}
    {% else %}
        {% set hashdiff_name = new_hashdiff_name %}
        {% set old_hashdiff_name = new_hashdiff_name + '_deprecated'%}
        {% set exclude_columns = [new_hashkey_name, new_hashdiff_name] %}
    {% endif %}

    {# Filter out the excluded columns by name #}
    {% set filtered_columns = [] %}
    {% for col in all_columns %}
    {% if col not in exclude_columns %}
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

    {% set select_clause = filtered_columns | join(', ') %}
    {{ log('SELECT clause: ' ~ select_clause, false) }}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {{ log('hash_config_dict' ~ hash_config_dict, false) }}

    {% set create_sql %}
    CREATE OR REPLACE TABLE {{ satellite_relation }} AS
        
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
            FROM {{ old_table_relation }} src
            LEFT JOIN {{ parent_relation }} parent
                ON src.{{ hashkey }} = parent.{{ join_hashkey_col }}
            WHERE src.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
            {# GROUP BY src.{{ hashkey }}, src.{{ ldts_col }}, src.{{ rsrc_alias }}, {{ datavault4dbt.print_list(hash_value_list)}} #}
        )    
        SELECT 
            hd.original_hashkey as {{ old_hashkey_name }},
            sat.{{ hashdiff_name }} as {{ old_hashdiff_name }},
            hd.{{ new_hashkey_name }},
            hd.{{ new_hashdiff_name }},
            sat.{{ select_clause }}
        FROM calculate_hd_correctly hd
        LEFT JOIN {{ old_table_relation }} sat
            ON hd.original_hashkey = sat.{{ hashkey }}
            AND hd.original_ldts = sat.{{ ldts_col }}

        UNION ALL

        SELECT 
            {{ hashkey }} as {{ old_hashkey_name }},
            {{ hashdiff_name }} as {{ old_hashdiff_name }},
            {{ hashkey }} as {{ new_hashkey_name }},
            {{ hashdiff_name }} as {{ new_hashdiff_name }},
            {{ select_clause }}
        FROM {{ old_table_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}