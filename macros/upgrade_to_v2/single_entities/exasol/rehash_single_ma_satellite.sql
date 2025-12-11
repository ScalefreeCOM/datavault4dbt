{#
    Usage example:
    dbt run-operation rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro exasol__rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set ma_satellite_relation = ref(ma_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var('datavault4dbt.ldts_alias', 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'HASHTYPE') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype},
        {"name": new_hashdiff_name, 
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add new hashkey and new hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=ma_satellite_relation, add_columns=new_hash_columns) }}
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


    {# generating the MERGE statement that populates the new columns. #}
    {% set update_sql = datavault4dbt.ma_satellite_update_statement(ma_satellite_relation=ma_satellite_relation,
                                                                   new_hashkey_name=new_hashkey_name,
                                                                   new_hashdiff_name=new_hashdiff_name,
                                                                   hashkey=hashkey, 
                                                                   business_key_list=business_key_list,
                                                                   ma_keys=ma_keys,
                                                                   ldts_col=ldts_col,
                                                                   hash_config_dict=hash_config_dict,
                                                                   parent_relation=parent_relation) %}

    {# Executing the MERGE statement. #}
    {{ log('Executing MERGE statement...'~update_sql, true) }}
    {{ '/* MERGE STATEMENT FOR ' ~ ma_satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('MERGE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'},
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_hkey1 = datavault4dbt.get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') %}
        {% do run_query(overwrite_hkey1) %}

        {% set overwrite_hkey2 = datavault4dbt.get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) %}
        {% do run_query(overwrite_hkey2) %}

        {% set overwrite_hdiff1 = datavault4dbt.get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') %}
        {% do run_query(overwrite_hdiff1) %}

        {% set overwrite_hdiff2 = datavault4dbt.get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff ) %}
        {% do run_query(overwrite_hdiff2) %}
        
        {% if drop_old_values %}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=ma_satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', output_logs) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}

{% macro exasol__ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}

    {% set ns = namespace(update_on_condition='', update_sql='', parent_already_rehashed=false) %}

    {% set prefixed_hashkey = 'sat.' ~ hashkey %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Check if parent is rehashed logic #}
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

    {%- set tmp_ns = namespace(main_hashkey_dict={}, hashdiff_names=[], hashdiff_dict={}) -%}

    {%- for column in hash_config_dict.keys() -%}
        {%- if not hash_config_dict[column].is_hashdiff -%}
            {%- do tmp_ns.main_hashkey_dict.update({column: hash_config_dict[column]}) -%}
        {%- elif hash_config_dict[column].is_hashdiff -%}
            {%- do tmp_ns.hashdiff_names.append(column) -%}
            {%- do tmp_ns.hashdiff_dict.update({column: hash_config_dict[column]}) -%}
        {%- endif -%}
    {%- endfor -%}

    {% set ns.update_sql %}
    MERGE INTO {{ ma_satellite_relation }} sat
    USING (

        SELECT 
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            {# Include Multi-Active Keys in the SELECT for the MERGE ON clause #}
            {# {{ datavault4dbt.print_list(ma_keys) }},  #}

            {% if new_hashkey_name not in hash_config_dict.keys() %}
                {# If Business Keys are not defined for parent entity, use new hashkey already existing in parent entitiy. #}
                parent.{{ select_hashkey_col }} as {{ new_hashkey_name }}
                ,{{ datavault4dbt.hash_columns(columns=hash_config_dict, main_hashkey_column=prefixed_hashkey, multi_active_key=ma_keys) }}

            {% else %}
                {%- set processed_hash_columns = datavault4dbt.process_hash_column_excludes(tmp_ns.main_hashkey_dict) -%}
                {{ datavault4dbt.hash_columns(columns=processed_hash_columns) }}
                
                {% set processed_hashdiff_columns = datavault4dbt.process_hash_column_excludes(tmp_ns.hashdiff_dict) -%}
                {{ datavault4dbt.hash_columns(columns=processed_hashdiff_columns, multi_active_key=ma_keys, main_hashkey_column=prefixed_hashkey) }},
      
            {% endif %}
            
            
        FROM {{ ma_satellite_relation }} sat
        LEFT JOIN (
            SELECT 
                {{ join_hashkey_col }},
                {{ select_hashkey_col }},
                {{ datavault4dbt.print_list(business_key_list) }}
            FROM {{ parent_relation }} 
        ) parent
            ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
        {#
           The GROUP BY in the original macro is unusual for this specific SELECT
           but is retained for fidelity. Exasol supports the syntax.
        #}
        GROUP BY sat.{{ hashkey }},
                 sat.{{ ldts_col }},
                 {# {{ datavault4dbt.print_list(ma_keys) }}, #}
                 {{ datavault4dbt.print_list(business_key_list, src_alias='parent') }}
                {% if new_hashkey_name not in hash_config_dict.keys() %}
                 , parent.{{ select_hashkey_col }}                 
                {% endif %}          

        UNION ALL
            
        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            {# Include Multi-Active Keys in the SELECT for the MERGE ON clause #}
            {{ datavault4dbt.print_list(ma_keys) }},
            sat.{{ hashkey }} AS {{ new_hashkey_name }},
            sat.{{ new_hashdiff_name | replace('_new', '') }} AS {{ new_hashdiff_name }}
        FROM {{ ma_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')          
                    
    ) nh
    {# Build the ON clause using Hashkey, LDTS, and MA Keys #}
    ON sat.{{ ldts_col }} = nh.{{ ldts_col }}
    AND sat.{{ hashkey }} = nh.{{ hashkey }}
    {% endset %}

    {# Add the multi-active keys to the ON condition #}
    {% for ma_key in ma_keys %}
        {% set where_condition %}
        AND sat.{{ datavault4dbt.escape_column_names(ma_key) }} = nh.{{ datavault4dbt.escape_column_names(ma_key) }}
        {% endset %}
        {% set ns.update_sql = ns.update_sql + where_condition %}
    {% endfor %}

    {# Add the final update statement #}
    {% set update_sql_set_clause %}
    WHEN MATCHED THEN
        UPDATE SET 
            sat.{{ new_hashkey_name}} = nh.{{ new_hashkey_name}},
            sat.{{ new_hashdiff_name}} = nh.{{ new_hashdiff_name }};
    {% endset %}

    {% set ns.update_sql = ns.update_sql + update_sql_set_clause %}

    {{ return(ns.update_sql) }}

{% endmacro %}