{#
    Works for Non-Historized Satellites either attached to Hubs or (NH)Links on Oracle.
    If attached to Hub: 
        Define Business Keys of Hub 
        OR Rehash Hub first, without overwriting hash values.

    If attached to (NH)Link:
        Rehash (NH)Link first, without overwriting hash values.

    Usage example:
    dbt run-operation rehash_single_nh_satellite --args '{nh_satellite: order_customer_n_ns, hashkey: HK_ORDER_CUSTOMER_NL, parent_entity: order_customer_nl, overwrite_hash_values: true}'
#}

{% macro oracle__rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set nh_satellite_relation = ref(nh_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}

    {# Oracle default for hashes is usually VARCHAR2(32) #}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR2(32)') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype}
    ]%}

    {# 1. Add new column #}
    {{ log('Executing ALTER TABLE statement (Adding new column)...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, add_columns=new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {% if datavault4dbt.is_something(business_keys) %}
        {# Ensuring business_keys is a list. #}
        {% if business_keys is iterable and business_keys is not string %}
            {% set business_key_list = business_keys %}
        {% else %}
            {% set business_key_list = [business_keys] %}
        {% endif %}

        {# Adding prefixes to column names for proper selection. #}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}

        {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys} %}
        
    {% else %}

        {% set hash_config_dict = none %}

    {% endif %}


    {# 2. Generate MERGE statement #}
    {% set update_sql = datavault4dbt.nh_satellite_update_statement(nh_satellite_relation=nh_satellite_relation,
                                                                    new_hashkey_name=new_hashkey_name,
                                                                    hashkey=hashkey, 
                                                                    ldts_col=ldts_col,
                                                                    hash_config_dict=hash_config_dict,
                                                                    parent_relation=parent_relation) %}

    {# Execute MERGE statement. #}
    {{ log('Executing UPDATE (MERGE) statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ nh_satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'}
    ]%}

    {# 3. Rename columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Rename existing to deprecated #}
        {% do run_query("ALTER TABLE " ~ nh_satellite_relation ~ " RENAME COLUMN " ~ hashkey ~ " TO " ~ hashkey ~ "_deprecated") %}
        
        {# Rename new to standard #}
        {% do run_query("ALTER TABLE " ~ nh_satellite_relation ~ " RENAME COLUMN " ~ new_hashkey_name ~ " TO " ~ hashkey) %}

        
        {# 4. Drop old columns #}
        {% if drop_old_values %}
            {{ log('Dropping deprecated columns...', output_logs) }}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten and old columns dropped!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro oracle__nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}

    {% set ns = namespace(parent_already_rehashed=false) %}

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
            {{ log('Parent already hashed, using rehashed value for ' ~ nh_satellite_relation.name, false) }}
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
    MERGE INTO {{ nh_satellite_relation }} dest
    USING (

        SELECT 
            sat.{{ hashkey }} as original_hashkey,
            sat.{{ ldts_col }} as original_ldts,
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
            sat.{{ hashkey }} as original_hashkey,
            sat.{{ ldts_col }} as original_ldts,
            sat.{{ hashkey }} AS {{ new_hashkey_name }}
        FROM {{ nh_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
    ) src
    ON (dest.{{ hashkey }} = src.original_hashkey AND dest.{{ ldts_col }} = src.original_ldts)
    
    WHEN MATCHED THEN
        UPDATE SET dest.{{ new_hashkey_name}} = src.{{ new_hashkey_name}}
        
    {% endset %}

    {{ return(merge_sql) }}

{% endmacro %}