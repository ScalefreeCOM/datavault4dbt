{#
    Usage example:
    dbt run-operation rehash_single_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro databricks__rehash_single_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys=none, src_ldts=none, src_rsrc=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {# SETUP VARIABLES #}
    {% set ldts_col = src_ldts or var('datavault4dbt.ldts_alias', 'ldts') %}
    
    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Create definition of new columns #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name, "data_type": hash_datatype},
        {"name": new_hashdiff_name, "data_type": hash_datatype}
    ]%}

    {# List normalization #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}


    {# Enable Delta Column Mapping #}
    {{ log('Enabling Delta Column Mapping...', output_logs) }}
    {% do run_query("ALTER TABLE " ~ satellite_relation ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')") %}


    {# Auto-Cleanup (Drop stuck columns) #}
    {% set existing_columns = adapter.get_columns_in_relation(satellite_relation) %}
    {% set existing_col_names = existing_columns | map(attribute='name') | list %}
    {% set potential_stuck_cols = [new_hashkey_name, new_hashdiff_name, hashkey + '_deprecated', hashdiff + '_deprecated'] %}

    {% for col_name in potential_stuck_cols %}
        {% if col_name in existing_col_names %}
            {{ log('Dropping stuck column ' ~ col_name, true) }}
            {% do run_query("ALTER TABLE " ~ satellite_relation ~ " DROP COLUMN " ~ col_name) %}
        {% endif %}
    {% endfor %}


    {# Add New Columns #}
    {{ log('Executing ALTER TABLE to add columns...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=satellite_relation, add_columns=new_hash_columns) }}


    {# Calculate Hashes (MERGE) #}
    
    {# Prepare Hash Config #}
    {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
    
    {% if datavault4dbt.is_something(business_keys) %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}
        {% set hash_config_dict = {
            new_hashkey_name: prefixed_business_keys,
            new_hashdiff_name: { "is_hashdiff": true, "columns": prefixed_payload }
        } %}
    {% else %}
        {% set hash_config_dict = {
            new_hashdiff_name: { "is_hashdiff": true, "columns": prefixed_payload }
        } %}
    {% endif %}

    {% set update_sql = adapter.dispatch('satellite_update_statement', 'datavault4dbt')(
                            satellite_relation=satellite_relation,
                            new_hashkey_name=new_hashkey_name,
                            new_hashdiff_name=new_hashdiff_name,
                            hashkey=hashkey, 
                            ldts_col=ldts_col,
                            hash_config_dict=hash_config_dict,
                            parent_relation=parent_relation) %}

    {{ log('Executing MERGE statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('MERGE statement completed!', output_logs) }}


    {# Handle Renames #}
    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}, {"name": hashdiff + '_deprecated'}] %}

    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values...', output_logs) }}

        {# Rename Hashkey #}
        {% do run_query(datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated')) %}
        {% do run_query(datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey)) %}

        {# Rename Hashdiff #}
        {% do run_query(datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated')) %}
        {% do run_query(datavault4dbt.get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff)) %}
        
        {# Drop Old Values #}
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {% for col in columns_to_drop %}
                {% do run_query("ALTER TABLE " ~ satellite_relation ~ " DROP COLUMN IF EXISTS " ~ col.name) %}
            {% endfor %}
            {{ log('Cleaned up old columns.', output_logs) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}

{% macro databricks__satellite_update_statement(satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, ldts_col, hash_config_dict, parent_relation) %}

    {% set ns = namespace(parent_already_rehashed=false) %}

    {# Resolve RSRC here #}
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
    MERGE INTO {{ satellite_relation }} AS sat
    USING (

        SELECT 
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            
            {% if new_hashkey_name not in hash_config_dict.keys() %}
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
            
    ) AS nh
    
    ON nh.{{ ldts_col }} = sat.{{ ldts_col }}
    AND nh.{{ hashkey }} = sat.{{ hashkey }}
    
    WHEN MATCHED THEN
        UPDATE SET 
            {{ new_hashkey_name}} = nh.{{ new_hashkey_name }},
            {{ new_hashdiff_name}} = nh.{{ new_hashdiff_name }}

    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}