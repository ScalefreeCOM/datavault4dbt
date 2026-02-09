{#
    Usage example:
    dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'
#}

{% macro databricks__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    
    {# Initialize namespace variables #}
    {% set ns = namespace(hub_hashkeys=[], new_hash_columns=[{"name": new_link_hashkey_name, "data_type": hash_datatype}], columns_to_drop=[{"name": link_hashkey + '_deprecated'}]) %}

    {% set link_relation = ref(link) %}

    {# Prepare Configuration #}
    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% set new_hub_hashkey_name = hub.hub_hashkey ~ '_new' %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "new_hashkey_name": new_hub_hashkey_name,
            "hub_name": hub.hub_name,
            "hub_join_alias": hub_join_alias,
            "prefixed_business_keys": prefixed_business_keys
        } %}

        {% do ns.hub_hashkeys.append(hub_hashkey_dict) %}

        {% set new_hash_col_dict = {
            "name": new_hub_hashkey_name,
            "data_type": hash_datatype
        } %}

        {% do ns.new_hash_columns.append(new_hash_col_dict) %}
        
        {# Add to columns to drop later if needed #}
        {% do ns.columns_to_drop.append({"name": hub.hub_hashkey + '_deprecated'}) %}

    {% endfor %}


    {# Enable Delta Column Mapping #}
    {{ log('Enabling Delta Column Mapping on ' ~ link_relation ~ '...', output_logs) }}
    {% set enable_mapping_sql %}
        ALTER TABLE {{ link_relation }} SET TBLPROPERTIES (
           'delta.columnMapping.mode' = 'name',
           'delta.minReaderVersion' = '2',
           'delta.minWriterVersion' = '5'
        )
    {% endset %}
    {% do run_query(enable_mapping_sql) %}


    {# Auto-Cleanup (Drop _new AND _deprecated columns from failed runs) #}
    {% set existing_columns = adapter.get_columns_in_relation(link_relation) %}
    {% set existing_col_names = existing_columns | map(attribute='name') | list %}
    
    {# Build a list of all potential stuck columns #}
    {% set potential_stuck_cols = [new_link_hashkey_name, link_hashkey + '_deprecated'] %}
    
    {% for hub in hub_config %}
        {% do potential_stuck_cols.append(hub.hub_hashkey ~ '_new') %}
        {% do potential_stuck_cols.append(hub.hub_hashkey ~ '_deprecated') %}
    {% endfor %}

    {# Loop through and drop if they exist #}
    {% for col_name in potential_stuck_cols %}
        {% if col_name in existing_col_names %}
            {{ log('Dropping stuck column ' ~ col_name, true) }}
            {% do run_query("ALTER TABLE " ~ link_relation ~ " DROP COLUMN " ~ col_name) %}
        {% endif %}
    {% endfor %}


    {# Add New Columns #}
    {{ log('Executing ALTER TABLE statement to add columns...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=link_relation, add_columns=ns.new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}


    {# Calculate Hashes (Merge) #}
    {% set update_sql = datavault4dbt.link_update_statement(link_relation=link_relation,
                                                            hub_hashkeys=ns.hub_hashkeys,
                                                            link_hashkey=link_hashkey,
                                                            new_link_hashkey_name=new_link_hashkey_name,
                                                            additional_hash_input_cols=additional_hash_input_cols) %}

    {{ log('Executing MERGE statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('MERGE statement completed!', output_logs) }}


    {# Handle Renames (Split Operations) #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# 6a. Rename Link Hashkeys #}
        {% set rename_link_old = datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=link_hashkey, new_col_name=link_hashkey + '_deprecated') %}
        {% do run_query(rename_link_old) %}
        
        {% set rename_link_new = datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=new_link_hashkey_name, new_col_name=link_hashkey) %}
        {% do run_query(rename_link_new) %}

        {# 6b. Rename Hub Hashkeys #}
        {% for hub_hashkey in ns.hub_hashkeys %}
            {% set rename_hub_old = datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=hub_hashkey.current_hashkey_name, new_col_name=hub_hashkey.current_hashkey_name + '_deprecated') %}
            {% do run_query(rename_hub_old) %}
            
            {% set rename_hub_new = datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=hub_hashkey.new_hashkey_name, new_col_name=hub_hashkey.current_hashkey_name) %}
            {% do run_query(rename_hub_new) %}
        {% endfor %}

        {# 6c. Drop Old Values (Loop through drops) #}
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {% for col in ns.columns_to_drop %}
                {% set drop_sql %}
                    ALTER TABLE {{ link_relation }} DROP COLUMN IF EXISTS {{ col.name }}
                {% endset %}
                {% do run_query(drop_sql) %}
            {% endfor %}
            {{ log('Existing Hash values overwritten and old columns dropped!', output_logs) }}
        {% endif %}

    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}


{% macro databricks__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}

    {% set ns = namespace(link_hashkey_input_cols=[], hash_config_dict={}) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Prepare Hash Configuration #}
    {% for hub_hashkey in hub_hashkeys %}
        {% do ns.hash_config_dict.update({hub_hashkey.new_hashkey_name: hub_hashkey.prefixed_business_keys}) %}
        {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + hub_hashkey.prefixed_business_keys %}
    {% endfor %}

    {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + additional_hash_input_cols %}
    {% do ns.hash_config_dict.update({new_link_hashkey_name: ns.link_hashkey_input_cols}) %}

    
    {# Construct MERGE Statement #}
    {% set merge_sql %}
    MERGE INTO {{ link_relation }} AS link
    USING (

        SELECT
            link.{{ link_hashkey }},
            {# Generates all the new hash key columns #}
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }}
        FROM {{ link_relation }} link
    
        {% for hub in hub_hashkeys %}
            
            {% set hub_ns = namespace(hub_already_rehashed=false) %}

            {# Query Hub columns to check if the deprecated column exists #}
            {% set all_hub_columns = adapter.get_columns_in_relation(ref(hub.hub_name)) %}
            {% for column in all_hub_columns %}
                {% if column.name|lower == hub.current_hashkey_name|lower + '_deprecated' %}
                    {% set hub_ns.hub_already_rehashed = true %}
                {% endif %}
            {% endfor %}

            {% if hub_ns.hub_already_rehashed %}
                {% set join_hashkey_col = hub.current_hashkey_name + '_deprecated' %}
            {% else %}
                {% set join_hashkey_col = hub.current_hashkey_name %}
            {% endif %}

            {# Perform the LEFT JOIN to the Hub table #}
            LEFT JOIN {{ ref(hub.hub_name) }} {{ hub.hub_join_alias }}
                ON link.{{ hub.current_hashkey_name }} = {{ hub.hub_join_alias }}.{{ join_hashkey_col }}

        {% endfor %}

        WHERE link.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        {# Pass-through for Unknown/Error records #}
        SELECT 
            link.{{ link_hashkey }}
            {% for hub in hub_hashkeys %}
                , link.{{ hub.current_hashkey_name }} as {{ hub.new_hashkey_name }}
            {% endfor %}
            , link.{{ link_hashkey }} as {{ new_link_hashkey_name }}
        FROM {{ link_relation }} link
        WHERE link.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    ) AS nh
    ON link.{{ link_hashkey }} = nh.{{ link_hashkey }}
    
    WHEN MATCHED THEN
        UPDATE SET 
            {{ new_link_hashkey_name }} = nh.{{ new_link_hashkey_name }}
            {% for hub in hub_hashkeys %}
                , {{ hub.new_hashkey_name }} = nh.{{ hub.new_hashkey_name }}
            {% endfor %}

    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}