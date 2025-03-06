{#
    Usage example:
    dbt run-operation rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set ma_satellite_relation = ref(ma_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype},
        {"name": new_hashdiff_name, 
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add new hashkey and new hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ alter_relation_add_remove_columns(relation=ma_satellite_relation, add_columns=new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

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
                "columns": prefixed_payload
                }
            } %}


    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql = datavault4dbt.ma_satellite_update_statement(ma_satellite_relation=ma_satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                new_hashdiff_name=new_hashdiff_name,
                                                hashkey=hashkey, 
                                                business_key_list=business_key_list,
                                                ma_keys=ma_keys,
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'},
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
        {{ get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') }}
        {{ get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') }}
        {{ get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) }}
        {{ get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff ) }}
        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        
        {% if drop_old_values %}
            {{ alter_relation_add_remove_columns(relation=ma_satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', output_logs) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}

    {{ adapter.dispatch('ma_satellite_update_statement', 'datavault4dbt')(ma_satellite_relation=ma_satellite_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       new_hashdiff_name=new_hashdiff_name,
                                                                       hashkey=hashkey, 
                                                                       business_key_list=business_key_list,
                                                                       ma_keys=ma_keys,
                                                                       ldts_col=ldts_col,
                                                                       hash_config_dict=hash_config_dict,
                                                                       parent_relation=parent_relation) }}

{% endmacro %}


{% macro default__ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}

    {% set ns = namespace(update_where_condition='') %}

    {% set prefixed_hashkey = 'sat.' ~ hashkey %}

    {% set update_sql %}
    UPDATE {{ ma_satellite_relation }} sat
    SET 
        {{ new_hashkey_name}} = nh.{{ new_hashkey_name}},
        {{ new_hashdiff_name}} = nh.{{ new_hashdiff_name }}
    FROM (

        SELECT 
            sat.{{ datavault4dbt.escape_column_names(hashkey) }},
            sat.{{ datavault4dbt.escape_column_names(ldts_col) }},
            {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(ma_keys)) }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict, main_hashkey_column=prefixed_hashkey, multi_active_key=ma_keys) }}
        FROM {{ ma_satellite_relation }} sat
        LEFT JOIN (
            SELECT 
                {{ datavault4dbt.escape_column_names(hashkey) }},
                {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(business_key_list)) }}
            FROM {{ parent_relation }} 
        ) parent
            ON sat.{{ datavault4dbt.escape_column_names(hashkey) }} = parent.{{ datavault4dbt.escape_column_names(hashkey) }}
            
    ) nh
    WHERE nh.{{ datavault4dbt.escape_column_names(ldts_col) }} = sat.{{ datavault4dbt.escape_column_names(ldts_col) }}
    AND nh.{{ datavault4dbt.escape_column_names(hashkey) }} = sat.{{ datavault4dbt.escape_column_names(hashkey) }}
    {% endset %}

    {% for ma_key in ma_keys %}

        {% set where_condition %}
            AND nh.{{ datavault4dbt.escape_column_names(ma_key) }} = sat.{{ datavault4dbt.escape_column_names(ma_key) }}
        {% endset %}

        {% set ns.update_where_condition = ns.update_where_condition + where_condition %}

    {% endfor %}

    {% set update_sql = update_sql + ns.update_where_condition %}

    {{ return(update_sql) }}

{% endmacro %}