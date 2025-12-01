{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}
{% macro databricks__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}
    {% set new_hashkey_name = hashkey + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# Enable Delta Column Mapping Automatically #}
    {{ log('Enabling Delta Column Mapping on ' ~ hub_relation ~ '...', output_logs) }}
    {% set enable_mapping_sql %}
        ALTER TABLE {{ hub_relation }} SET TBLPROPERTIES (
           'delta.columnMapping.mode' = 'name',
           'delta.minReaderVersion' = '2',
           'delta.minWriterVersion' = '5'
        )
    {% endset %}
    {% do run_query(enable_mapping_sql) %}


    {# Auto-Cleanup (Drop _new column if it exists from a failed run) #}
    {% set existing_columns = adapter.get_columns_in_relation(hub_relation) %}
    {% set existing_col_names = existing_columns | map(attribute='name') | list %}
    
    {% if new_hashkey_name in existing_col_names %}
        {{ log('Found leftover column ' ~ new_hashkey_name ~ '. Dropping it to ensure clean state...', output_logs) }}
        {% set cleanup_sql %}
            ALTER TABLE {{ hub_relation }} DROP COLUMN {{ new_hashkey_name }}
        {% endset %}
        {% do run_query(cleanup_sql) %}
    {% endif %}


    {# Add the new column #}
    {{ log('Adding new hashkey column...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=hub_relation, add_columns=new_hash_col) }}
    {{ log('Column added successfully.', output_logs) }}


    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition for new hashkey. #}
    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}


    {# Calculate new hashes #}
    {% set update_sql = datavault4dbt.hub_update_statement(hub_relation=hub_relation,
                                                           new_hashkey_name=new_hashkey_name,
                                                           hashkey=hashkey,
                                                           hash_config_dict=hash_config_dict) %}

    {{ log('Executing MERGE update statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('Update calculation completed!', output_logs) }}


    {# Handle Renames #}
    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}

    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values...', output_logs) }}

        {# Rename Original -> Deprecated #}
        {% set rename_old_sql = datavault4dbt.get_rename_column_sql(relation=hub_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') %}
        {% do run_query(rename_old_sql) %}
        
        {# Rename New -> Original #}
        {% set rename_new_sql = datavault4dbt.get_rename_column_sql(relation=hub_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) %}
        {% do run_query(rename_new_sql) %}
        
        {% if drop_old_values == 'true' %}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}
            {{ log('Old deprecated column dropped.', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}
    
{% endmacro %}



{% macro databricks__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set merge_sql %}
    MERGE INTO {{ hub_relation }} AS hub
    USING (
        SELECT 
            hub.{{ hashkey }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ hub_relation }} hub  
        WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT 
            hub.{{ hashkey }},
            hub.{{ hashkey }} as {{ new_hashkey_name }}
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')          
    ) AS nh
    ON hub.{{ hashkey }} = nh.{{ hashkey }}
    WHEN MATCHED THEN 
        UPDATE SET hub.{{ new_hashkey_name }} = nh.{{ new_hashkey_name }}
    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}