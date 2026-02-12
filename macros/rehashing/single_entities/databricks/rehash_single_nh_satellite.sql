{#
    Usage example:
    dbt run-operation rehash_single_nh_satellite --args '{nh_satellite: order_customer_n_ns, hashkey: HK_ORDER_CUSTOMER_NL, parent_entity: order_customer_nl, overwrite_hash_values: true}'
#}

{% macro databricks__rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, src_ldts=none, src_rsrc=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set nh_satellite_relation = ref(nh_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = src_ldts or var('datavault4dbt.ldts_alias', 'ldts') %}
    
    {% set new_hashkey_name = hashkey + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set new_hash_columns = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# Only try to prefix if business_keys actually has content #}
    {% if business_keys is defined and business_keys is not none and business_keys | length > 0 %}
        {% set business_key_list = business_keys if business_keys is iterable and business_keys is not string else [business_keys] %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}
        {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys} %}
    {% else %}
        {% set hash_config_dict = none %}
    {% endif %}

    {# Enable Delta Column Mapping #}
    {{ log('Enabling Delta Column Mapping...', output_logs) }}
    {% do run_query("ALTER TABLE " ~ nh_satellite_relation ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')") %}

    {# Auto-Cleanup #}
    {% set existing_columns = adapter.get_columns_in_relation(nh_satellite_relation) %}
    {% set existing_col_names = existing_columns | map(attribute='name') | list %}
    {% set potential_stuck_cols = [new_hashkey_name, hashkey + '_deprecated'] %}

    {% for col_name in potential_stuck_cols %}
        {% if col_name in existing_col_names %}
            {{ log('Dropping stuck column ' ~ col_name, true) }}
            {% do run_query("ALTER TABLE " ~ nh_satellite_relation ~ " DROP COLUMN " ~ col_name) %}
        {% endif %}
    {% endfor %}

    {# Add New Columns #}
    {{ log('Adding new columns...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, add_columns=new_hash_columns) }}

    {# Calculate Hashes (MERGE) #}
    {% set update_sql = adapter.dispatch('nh_satellite_update_statement','datavault4dbt')(nh_satellite_relation=nh_satellite_relation,
                                                                    new_hashkey_name=new_hashkey_name,
                                                                    hashkey=hashkey, 
                                                                    ldts_col=ldts_col,
                                                                    parent_relation=parent_relation,
                                                                    hash_config_dict=hash_config_dict) %}

    {{ log('Executing MERGE...', output_logs) }}
    {% do run_query(update_sql) %}

    {# Handle Renames #}
    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}] %}

    {% if overwrite_hash_values %}
        {{ log('Renaming columns...', output_logs) }}
        {% do run_query(datavault4dbt.get_rename_column_sql(relation=nh_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated')) %}
        {% do run_query(datavault4dbt.get_rename_column_sql(relation=nh_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey)) %}
        
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {% for col in columns_to_drop %}
                {% do run_query("ALTER TABLE " ~ nh_satellite_relation ~ " DROP COLUMN IF EXISTS " ~ col.name) %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}

{% macro databricks__nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}

    {% set ns = namespace(parent_already_rehashed=false) %}
    
    {# DEFINE RSRC HERE #}
    {% set rsrc_alias = src_rsrc or var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Check if parent is rehashed #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}

    {# Construct MERGE Statement #}
    {% set merge_sql %}
    MERGE INTO {{ nh_satellite_relation }} AS sat
    USING (

        SELECT 
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            {% if datavault4dbt.is_something(hash_config_dict) %}
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
            {% else %}
                parent.{{ select_hashkey_col }} as {{ new_hashkey_name }}
            {% endif %}
        FROM {{ nh_satellite_relation }} sat
        LEFT JOIN {{ parent_relation }} parent
            ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}') 

        UNION ALL

        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }}, 
            sat.{{ hashkey }} AS {{ new_hashkey_name }}
        FROM {{ nh_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}') 
            
    ) AS nh
    
    ON nh.{{ ldts_col }} = sat.{{ ldts_col }}
    AND nh.{{ hashkey }} = sat.{{ hashkey }}
    
    WHEN MATCHED THEN
        UPDATE SET 
            {{ new_hashkey_name}} = nh.{{ new_hashkey_name}}

    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}