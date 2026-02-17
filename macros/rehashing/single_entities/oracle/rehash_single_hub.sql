{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro oracle__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {% set new_hashkey_name = hashkey + '_new' %}
    
    {# Oracle default for hashes is usually VARCHAR2(32) for MD5 or 64 for SHA256 #}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR2(32)') %}

    {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# 1. Add new column #}
    {{ log('Executing ALTER TABLE statement (Adding new column)...', output_logs) }}
    {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, add_columns=new_hash_col) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# Ensure business_keys is a list #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition #}
    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}

    {# 2. Generate update statement (Uses MERGE for Oracle) #}
    {% set update_sql = datavault4dbt.hub_update_statement(hub_relation=hub_relation,
                                                           new_hashkey_name=new_hashkey_name,
                                                           hashkey=hashkey,
                                                           hash_config_dict=hash_config_dict) %}

    {# Execute Update #}
    {{ log('Executing UPDATE (MERGE) statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}

    {{ log('Overwrite_hash_values for hubs: ' ~ overwrite_hash_values, output_logs ) }}

    {# 3. Rename columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
            ALTER TABLE {{ hub_relation }} RENAME COLUMN {{ hashkey }} TO {{ hashkey }}_deprecated;
            ALTER TABLE {{ hub_relation }} RENAME COLUMN {{ new_hashkey_name }} TO {{ hashkey }};
        {% endset %}

        {# Since Oracle usually requires separate execution for these DDLs in dbt, we run them sequentially #}
        {% do run_query("ALTER TABLE " ~ hub_relation ~ " RENAME COLUMN " ~ hashkey ~ " TO " ~ hashkey ~ "_deprecated") %}
        {% do run_query("ALTER TABLE " ~ hub_relation ~ " RENAME COLUMN " ~ new_hashkey_name ~ " TO " ~ hashkey) %}

        {# 4. Drop old column #}
        {% if drop_old_values == 'true' or drop_old_values == true %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten and old column dropped!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}
    
{% endmacro %}


{% macro oracle__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}
    
    {# Oracle uses MERGE instead of UPDATE FROM #}
    {% set update_sql %}
    MERGE INTO {{ hub_relation }} dest
    USING (
        SELECT 
            hub.{{ hashkey }} as original_hashkey,
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }} as calculated_hash
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT 
            hub.{{ hashkey }} as original_hashkey,
            hub.{{ hashkey }} as calculated_hash
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
    ) src
    ON (dest.{{ hashkey }} = src.original_hashkey)
    WHEN MATCHED THEN
        UPDATE SET dest.{{ new_hashkey_name }} = src.calculated_hash
    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}