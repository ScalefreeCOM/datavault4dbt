{#
    Usage example:
    dbt run-operation databricks__rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}
{% macro databricks__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}
    
    {% set new_hashkey_name = hashkey ~ '_new' %}
    {% set dep_hashkey_name = hashkey ~ '_deprecated' %}
    
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# Enable Delta Column Mapping#}
    {{ log('Enabling Delta Column Mapping on ' ~ hub_relation, output_logs) }}
    {% do run_query("ALTER TABLE " ~ hub_relation ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')") %}


    {# Standard Cleanup #}
    {% do run_query("ALTER TABLE " ~ hub_relation ~ " DROP COLUMN IF EXISTS `" ~ new_hashkey_name ~ "`") %}


    {#  Add New Column #}
    {{ log('Adding new hashkey column...', output_logs) }}
    {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, add_columns=new_hash_col) }}


    {#  Calculate Hashes #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}

    {% set update_sql = adapter.dispatch('hub_update_statement', 'datavault4dbt')(
                                            hub_relation=hub_relation,
                                            new_hashkey_name=new_hashkey_name,
                                            hashkey=hashkey,
                                            hash_config_dict=hash_config_dict) %}

    {{ log('Executing MERGE update statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('Update calculation completed!', output_logs) }}

    {% set columns_to_drop = [{"name": dep_hashkey_name}]%}

    {# Handle Renames #}
    {% if overwrite_hash_values %}
        {{ log('Renaming columns...', output_logs) }}

        {% if drop_old_values %}
            {% do run_query("ALTER TABLE " ~ hub_relation ~ " DROP COLUMN IF EXISTS `" ~ hashkey ~ "`") %}
            {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=new_hashkey_name, new_col_name=hashkey)) %}
        
        {% else %}
            {% do run_query("ALTER TABLE " ~ hub_relation ~ " DROP COLUMN IF EXISTS `" ~ dep_hashkey_name ~ "`") %}
            
            {# Rename Old -> Deprecated #}
            {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=hashkey, new_col_name=dep_hashkey_name)) %}
            
            {# Rename New -> Standard #}
            {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=new_hashkey_name, new_col_name=hashkey)) %}
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
        WITH source_data AS (
            SELECT 
                hub.{{ hashkey }},
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
            FROM {{ hub_relation }} hub  
            WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        ),
        
        ghost_records AS (
            SELECT 
                hub.{{ hashkey }},
                hub.{{ hashkey }} as {{ new_hashkey_name }}
            FROM {{ hub_relation }} hub
            WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')          
        )
        
        SELECT * FROM source_data
        UNION ALL
        SELECT * FROM ghost_records

    ) AS nh
    ON hub.{{ hashkey }} = nh.{{ hashkey }}
    WHEN MATCHED THEN 
        UPDATE SET hub.{{ new_hashkey_name }} = nh.{{ new_hashkey_name }}
    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}