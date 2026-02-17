{#
    Usage example:
    dbt run-operation databricks__rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro databricks__rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, src_ldts=none, src_rsrc=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set ma_satellite_relation = ref(ma_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {# --- SETUP VARIABLES --- #}
    {% set ldts_col = src_ldts or var('datavault4dbt.ldts_alias', 'ldts') %}
    
    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {% set new_hash_columns = [
        {"name": new_hashkey_name, "data_type": hash_datatype},
        {"name": new_hashdiff_name, "data_type": hash_datatype}
    ]%}

    {# List normalization #}
    {% set business_key_list = business_keys if business_keys is iterable and business_keys is not string else [business_keys] %}
    {% set ma_keys_list = ma_keys if ma_keys is iterable and ma_keys is not string else [ma_keys] %}


    {# --- STEP 1: Enable Delta Column Mapping --- #}
    {{ log('Enabling Delta Column Mapping...', output_logs) }}
    {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')") %}


    {# --- STEP 2: Auto-Cleanup (Initial) --- #}
    {% set potential_stuck_cols = [new_hashkey_name, new_hashdiff_name, hashkey + '_deprecated', hashdiff + '_deprecated'] %}
    {% for col_name in potential_stuck_cols %}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " DROP COLUMN IF EXISTS " ~ col_name) %}
    {% endfor %}


    {# --- STEP 3: Add New Columns --- #}
    {{ log('Adding new columns...', output_logs) }}
    {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=ma_satellite_relation, add_columns=new_hash_columns) }}


    {# --- STEP 4: Calculate Hashes (MERGE) --- #}
    
    {# Prepare Inputs #}
    {% set is_hashdiff = true %}
    {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
    {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}
    {% set prefixed_ma_keys = datavault4dbt.prefix(columns=ma_keys_list, prefix_str='sat').split(',') %}
    
    {# Using the logic that worked previously (BK + MA Keys) #}
    {% set pk_input_list = prefixed_business_keys + prefixed_ma_keys %}

    {# Config for HashDiff only #}
    {% set hashdiff_config = {
        new_hashdiff_name: {
            "is_hashdiff": is_hashdiff, 
            "columns": prefixed_payload
        }
    } %}

    {# Call Update Macro via Dispatch #}
    {% set update_sql = adapter.dispatch('ma_satellite_update_statement', 'datavault4dbt')(
                                            ma_satellite_relation=ma_satellite_relation,
                                            new_hashkey_name=new_hashkey_name,
                                            new_hashdiff_name=new_hashdiff_name,
                                            hashkey=hashkey, 
                                            business_key_list=business_key_list,
                                            ma_keys=ma_keys_list,
                                            ldts_col=ldts_col,
                                            pk_input_list=pk_input_list,
                                            hashdiff_config=hashdiff_config,
                                            parent_relation=parent_relation) %}

    {{ log('Executing MERGE statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('MERGE statement completed!', output_logs) }}


    {# --- STEP 5: Handle Renames (With Safety Drops) --- #}
    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}, {"name": hashdiff + '_deprecated'}] %}

    {% if overwrite_hash_values %}
        {{ log('Renaming columns...', output_logs) }}
        
        {# SAFETY FIX: Drop the target columns first to prevent 'Already Exists' error on dirty tables #}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " DROP COLUMN IF EXISTS " ~ hashkey + '_deprecated') %}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " DROP COLUMN IF EXISTS " ~ hashdiff + '_deprecated') %}

        {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated')) %}
        {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey)) %}
        {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated')) %}
        {% do run_query(datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff)) %}
        
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {% for col in columns_to_drop %}
                {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " DROP COLUMN IF EXISTS " ~ col.name) %}
            {% endfor %}
        {% endif %}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro databricks__ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, pk_input_list, hashdiff_config, parent_relation) %}

    {% set ns = namespace(parent_already_rehashed=false) %}
    
    {# Define Variables #}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set prefixed_hashkey = 'sat.' ~ hashkey %}

    {# Check if parent is already rehashed #}
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
        {% set select_hashkey_col = none %} 
    {% endif %}

    {# --- MERGE with Stage-Style CTEs --- #}
    {% set merge_sql %}
    MERGE INTO {{ ma_satellite_relation }} AS sat
    USING (
        WITH 
        
        {# 1. Parent Data #}
        parent_lookup AS (
            SELECT 
                {{ join_hashkey_col }} AS join_key,
                {% if select_hashkey_col is not none %}
                    {{ select_hashkey_col }} AS new_hk_val,
                {% endif %}
                {{ datavault4dbt.print_list(business_key_list) }}
            FROM {{ parent_relation }}
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ join_hashkey_col }} ORDER BY 1) = 1
        ),

        {# 2. Main Hash Key Calculation (Joins Sat & Parent) #}
        main_pk_prep AS (
            SELECT 
                sat.{{ hashkey }},
                sat.{{ ldts_col }},
                
                {# Manually loop MA keys #}
                {% for ma_key in ma_keys %} sat.{{ ma_key }}, {% endfor %}

                {# Primary Key Logic #}
                {% if select_hashkey_col is none %}
                     {{ datavault4dbt.hash(columns=pk_input_list, alias=new_hashkey_name) }},
                {% else %}
                     parent.new_hk_val AS {{ new_hashkey_name }},
                {% endif %}
                
                1 as ignore_me

            FROM {{ ma_satellite_relation }} sat
            LEFT JOIN parent_lookup parent
                ON sat.{{ hashkey }} = parent.join_key
            WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        ),

        {# 3. Multi-Active HashDiff Prep (Grouped) #}
        ma_hashdiff_prep AS (
            SELECT 
                {{ hashkey }},
                {{ ldts_col }},
                {# Calculate HashDiff #}
                {{ datavault4dbt.hash_columns(columns=hashdiff_config, multi_active_key=ma_keys, main_hashkey_column=prefixed_hashkey) }}
            
            FROM {{ ma_satellite_relation }} sat
            WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
            {# Critical Grouping for Multi-Active #}
            GROUP BY {{ hashkey }}, {{ ldts_col }}
        ),

        {# 4. Source Data (Join PK and Diff) #}
        source_data AS (
            SELECT
                main.{{ hashkey }},
                main.{{ ldts_col }},
                {# MA Keys BEFORE HashKey #}
                {% for ma_key in ma_keys %} main.{{ ma_key }}, {% endfor %}
                main.{{ new_hashkey_name }},
                
                diff.{{ new_hashdiff_name }}
            FROM main_pk_prep main
            LEFT JOIN ma_hashdiff_prep diff
                ON main.{{ hashkey }} = diff.{{ hashkey }}
                AND main.{{ ldts_col }} = diff.{{ ldts_col }}
        ),

        {# 5. Ghost Records #}
        ghost_records AS (
             SELECT
                sat.{{ hashkey }},
                sat.{{ ldts_col }},
                {% for ma_key in ma_keys %} sat.{{ ma_key }}, {% endfor %}
                
                CAST(sat.{{ hashkey }} AS {{ hash_datatype }}) AS {{ new_hashkey_name }},
                CAST(sat.{{ new_hashdiff_name | replace('_new', '') }} AS {{ hash_datatype }}) AS {{ new_hashdiff_name }}
            
            FROM {{ ma_satellite_relation }} sat
            WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        ),

        {# 6. Final Union #}
        final_union AS (
            SELECT * FROM source_data
            UNION ALL
            SELECT * FROM ghost_records
        )

        SELECT * FROM final_union
        {# Final Safety Net #}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {{ hashkey }}, {{ ldts_col }}
            {% for ma_key in ma_keys %} , {{ ma_key }} {% endfor %}
            ORDER BY 1
        ) = 1

    ) AS nh
    
    {# Join Condition #}
    ON sat.{{ hashkey }} = nh.{{ hashkey }}
    AND sat.{{ ldts_col }} = nh.{{ ldts_col }}
    {% for ma_key in ma_keys %}
        AND sat.{{ datavault4dbt.escape_column_names(ma_key) }} = nh.{{ datavault4dbt.escape_column_names(ma_key) }}
    {% endfor %}

    WHEN MATCHED THEN
        UPDATE SET 
            {{ new_hashkey_name }} = nh.{{ new_hashkey_name }},
            {{ new_hashdiff_name }} = nh.{{ new_hashdiff_name }}

    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}