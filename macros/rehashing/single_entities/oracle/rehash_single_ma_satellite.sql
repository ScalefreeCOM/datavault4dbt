{#
    Usage example:
    dbt run-operation rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro oracle__rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set ma_satellite_relation = ref(ma_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {# Oracle default for hashes is usually VARCHAR2(32) #}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR2(32)') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype},
        {"name": new_hashdiff_name, 
         "data_type": hash_datatype}
    ]%}

    {# 1. Add new columns #}
    {{ log('Executing ALTER TABLE statement (Adding new columns)...', output_logs) }}
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


    {# 2. Generate MERGE statement #}
    {% set update_sql = datavault4dbt.ma_satellite_update_statement(ma_satellite_relation=ma_satellite_relation,
                                                                    new_hashkey_name=new_hashkey_name,
                                                                    new_hashdiff_name=new_hashdiff_name,
                                                                    hashkey=hashkey, 
                                                                    business_key_list=business_key_list,
                                                                    ma_keys=ma_keys,
                                                                    ldts_col=ldts_col,
                                                                    hash_config_dict=hash_config_dict,
                                                                    parent_relation=parent_relation) %}

    {# Execute MERGE statement. #}
    {{ log('Executing UPDATE (MERGE) statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ ma_satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'},
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# 3. Rename columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Rename existing to deprecated #}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " RENAME COLUMN " ~ hashkey ~ " TO " ~ hashkey ~ "_deprecated") %}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " RENAME COLUMN " ~ hashdiff ~ " TO " ~ hashdiff ~ "_deprecated") %}
        
        {# Rename new to standard #}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " RENAME COLUMN " ~ new_hashkey_name ~ " TO " ~ hashkey) %}
        {% do run_query("ALTER TABLE " ~ ma_satellite_relation ~ " RENAME COLUMN " ~ new_hashdiff_name ~ " TO " ~ hashdiff) %}

        
        {# 4. Drop old columns #}
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=ma_satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten and old columns dropped!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro oracle__ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}

    {% set ns = namespace(update_where_condition='', parent_already_rehashed=false) %}

    {% set prefixed_hashkey = 'sat.' ~ hashkey %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}


    {#
        Check if parent entity is already rehashed (looking for "_deprecated" column).
        If yes, use the deprecated key for joining, but select the regular (new) key.
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('Parent already hashed, using rehashed value for ' ~ ma_satellite_relation.name, false) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}


    {% set merge_sql %}
    MERGE INTO {{ ma_satellite_relation }} dest
    USING (

        SELECT 
            sat.{{ hashkey }} as original_hashkey,
            sat.{{ ldts_col }} as original_ldts,

            {% if new_hashkey_name not in hash_config_dict.keys() %}
                {# If Business Keys are not defined for parent entity, use new hashkey already existing in parent entitiy. #}
                parent.{{ select_hashkey_col }} as {{ new_hashkey_name }},
            {% endif %}
            
            {{ datavault4dbt.hash_columns(columns=hash_config_dict, main_hashkey_column=prefixed_hashkey, multi_active_key=ma_keys) }}

        FROM {{ ma_satellite_relation }} sat
        LEFT JOIN (
            SELECT 
                {{ join_hashkey_col }},
                {{ datavault4dbt.print_list(business_key_list) }}
            FROM {{ parent_relation }} 
        ) parent
            ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
        {# 
           Grouping is required here because Multi-Active Satellites often calculate the HashDiff
           across all active rows for a specific Load Date/HashKey combination.
        #}
        GROUP BY sat.{{ hashkey }},
                 sat.{{ ldts_col }},
                 {{ datavault4dbt.print_list(business_key_list, src_alias='parent') }}
                {% if new_hashkey_name not in hash_config_dict.keys() %}
                 , parent.{{ select_hashkey_col }}                
                {% endif %}          

        UNION ALL
            
        {# Handle Ghost Records / Error Records: Pass-through #}
        SELECT
            sat.{{ hashkey }} as original_hashkey,
            sat.{{ ldts_col }} as original_ldts,
            sat.{{ hashkey }} AS {{ new_hashkey_name }},
            sat.{{ new_hashdiff_name | replace('_new', '') }} AS {{ new_hashdiff_name }}
        FROM {{ ma_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')                     
                    
    ) src
    ON (dest.{{ hashkey }} = src.original_hashkey AND dest.{{ ldts_col }} = src.original_ldts)
    
    WHEN MATCHED THEN
        UPDATE SET 
            dest.{{ new_hashkey_name }} = src.{{ new_hashkey_name}},
            dest.{{ new_hashdiff_name }} = src.{{ new_hashdiff_name }}

    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}