{#
    Usage example:
    dbt run-operation rehash_single_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro redshift__rehash_single_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR(32)') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype},
        {"name": new_hashdiff_name, 
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add new hashkey and new hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=satellite_relation, add_columns=new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {% if datavault4dbt.is_something(business_keys) %}
        {# Ensuring business_keys is a list. #}
        {% if business_keys is iterable and business_keys is not string %}
            {% set business_key_list = business_keys %}
        {% else %}
            {% set business_key_list = [business_keys] %}
        {% endif %}

        {# Adding prefixes to column names for proper selection. #}
        {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
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
        {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}

        {% set hash_config_dict = {
                new_hashdiff_name: {
                    "is_hashdiff": true,
                    "columns": prefixed_payload
                    }
                } %}

    {% endif %}

    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql = datavault4dbt.satellite_update_statement(satellite_relation=satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                new_hashdiff_name=new_hashdiff_name,
                                                hashkey=hashkey, 
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...'~update_sql, output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {% do run_query('COMMIT;') %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated', "new_name": hashkey},
        {"name": hashdiff + '_deprecated', "new_name": hashdiff}
    ]%}
    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
        {{ datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') }}
        {{ datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') }}
        {{ datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) }}
        {{ datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff ) }}
        {% endset %}
        {% do run_query(overwrite_sql) %}

        {% if drop_old_values %}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro redshift__satellite_update_statement(satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, ldts_col, hash_config_dict, parent_relation) %}

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
            {{ log('Parent already hashed, using rehashed value for ' ~ satellite_relation.name, false) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}

    {% set update_sql %}
    UPDATE {{ satellite_relation }} sat
    SET 
        {{ new_hashkey_name }} = nh.{{ new_hashkey_name }},
        {{ new_hashdiff_name }} = nh.{{ new_hashdiff_name }}
    FROM (

        SELECT 
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            
            {% if new_hashkey_name not in hash_config_dict.keys() %}
                {# If Business Keys are not defined for parent entity, use new hashkey already existing in parent entitiy. #}
                parent.{{ select_hashkey_col }} as {{ new_hashkey_name }},
            {% endif %}

            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ satellite_relation }} sat
        LEFT JOIN {{ parent_relation }} parent
            ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }} 
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            sat.{{ hashkey }} AS {{ new_hashkey_name }},
            sat.{{ new_hashdiff_name | replace('_new', '') }} AS {{ new_hashdiff_name }}
        FROM {{ satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}') 
        ) nh
    WHERE nh.{{ ldts_col }} = sat.{{ ldts_col }}
    AND nh.{{ hashkey }} = sat.{{ hashkey }}

    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}