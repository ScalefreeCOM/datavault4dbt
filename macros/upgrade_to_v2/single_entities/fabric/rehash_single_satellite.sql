{#
    Usage example:
    dbt run-operation rehash_single_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro fabric__rehash_single_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set old_hashkey_name = hashkey + '_deprecated' %}
    {% set old_hashdiff_name = hashdiff + '_deprecated' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARBINARY(8000)') %}

    {# Create definition of deprecated columns for ALTER statement. #}
    {% set old_hash_columns = [
        {"name": old_hashkey_name,
         "data_type": hash_datatype},
        {"name": old_hashdiff_name, 
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add deprecated hashkey and deprecated hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ alter_relation_add_remove_columns(relation=satellite_relation, add_columns=old_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# Update SQL statement to copy hashkey to _depr column  #}
    {% set depr_update_sql %}
        UPDATE {{ satellite_relation }}
        SET 
            {{ old_hashkey_name }} = {{ hashkey }},
            {{ old_hashdiff_name }} = {{ hashdiff }};

    {% endset %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ depr_update_sql ~ '*/' }}

    {% do run_query(depr_update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {% set is_hashdiff = true %}

    {# Adding prefixes to column names for proper selection. #}
    {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
    {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}


    {% if overwrite_hash_values %}

        {% set new_hashkey_name = hashkey %}
        {% set new_hashdiff_name = hashdiff %}
        
        {% set hash_config_dict = {
            new_hashkey_name: prefixed_business_keys, 
            new_hashdiff_name: {
                "is_hashdiff": is_hashdiff, 
                "columns": prefixed_payload
                }
            } %}
            
    {% else %}

        {% set hash_config_dict = {
            new_hashkey_name: prefixed_business_keys, 
            new_hashdiff_name: {
                "is_hashdiff": is_hashdiff, 
                "columns": prefixed_payload
                }
            } %}        
        
        {% set new_hash_columns = [
            {"name": new_hashkey_name,
            "data_type": hash_datatype},
            {"name": new_hashdiff_name, 
            "data_type": hash_datatype}
        ]%}

        {# ALTER existing satellite to add new hashkey and new hashdiff. #}
        {{ log('Executing ALTER TABLE statement...', output_logs) }}
        {{ alter_relation_add_remove_columns(relation=satellite_relation, add_columns=new_hash_columns) }}
        {{ log('ALTER TABLE statement completed!', output_logs) }}

    {% endif %}

    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql = satellite_update_statement(satellite_relation=satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                new_hashdiff_name=new_hashdiff_name,
                                                hashkey=hashkey, 
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...' ~ update_sql, true) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": old_hashkey_name},
        {"name": old_hashdiff_name}
    ]%}

    {# Deleting old hashkey #}
    {% if drop_old_values or not overwrite_hash_values %}
        {{ alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}
        {{ log('Existing Hash values overwritten!', true) }}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}

{% macro fabric__satellite_update_statement(satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, ldts_col, hash_config_dict, parent_relation) %}

    {% set old_hashkey_name = hashkey + '_deprecated' %}
    
    {% set old_hashdiff_name = new_hashdiff_name %}
    {% if old_hashdiff_name.endswith('_new') %}
        {% set old_hashdiff_name = old_hashdiff_name[:-4] %}
    {% endif %}
    {% set old_hashdiff_name = old_hashdiff_name + '_deprecated' %}
    
    {% set ns = namespace(parent_already_rehashed=false) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {#
        If parent entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
        hashkey column needs to be used for joining, and the regular hashkey should be selected. 

        Otherwise, the regular hashkey should be used for joining. 
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('parent_already hashed set to true for ' ~ satellite_relation.name, true) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = old_hashkey_name %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
    {% endif %}

    {% set update_sql %}

        WITH calculate_hd_correctly AS (
            SELECT 
                sat.{{ old_hashkey_name }},
                sat.{{ ldts_col }},

                {% if new_hashkey_name not in hash_config_dict.keys() %}
                    parent.{{ hashkey }} as {{ new_hashkey_name }} ,
                {% endif %}

                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }} {#, main_hashkey_column=prefixed_hashkey #}

            FROM {{ satellite_relation }} sat
            LEFT JOIN {{ parent_relation }} parent
                ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
            WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

            UNION ALL

            SELECT
                sat.{{ old_hashkey_name }},
                sat.{{ ldts_col }},
                sat.{{ old_hashkey_name }} AS {{ new_hashkey_name }},
                sat.{{ old_hashdiff_name }} AS {{ new_hashdiff_name }}
            FROM {{ satellite_relation }} sat
            WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')          
        ) 
        
        UPDATE {{ satellite_relation }}
        SET 
            {{ new_hashkey_name}} = nh.{{ new_hashkey_name}},
            {{ new_hashdiff_name}} = nh.{{ new_hashdiff_name }}  
        FROM {{ satellite_relation }} sat
        LEFT JOIN calculate_hd_correctly nh
            ON sat.{{ old_hashkey_name }} = nh.{{ old_hashkey_name }}
            AND sat.{{ ldts_col }} = nh.{{ ldts_col }}
    
    {% endset %}
   
    {{ return(update_sql) }}

{% endmacro %}