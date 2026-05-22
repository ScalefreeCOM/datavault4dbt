{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro exasol__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'HASHTYPE') %}

    {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# Alter existing Hub to add new hashkey column. (Calls the translated macro) #}
    {{ log('Executing ALTER TABLE statement to add column...', output_logs) }}
    {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, add_columns=new_hash_col) }}

    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition for new hashkey. #}
    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}

    {# Get update SQL statement to calculate new hashkey. (Calls the translated macro) #}
    {% set update_sql = datavault4dbt.hub_update_statement(hub_relation=hub_relation,
                                                           new_hashkey_name=new_hashkey_name,
                                                           hashkey=hashkey,
                                                           hash_config_dict=hash_config_dict) %}

    {# Executing the UPDATE/MERGE statement. #}
    {{ log('Executing MERGE statement...', output_logs) }}
    {{ '/* MERGE STATEMENT FOR ' ~ hub ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('MERGE statement completed!', output_logs) }}

    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}

    {{ log('Overwrite_hash_values for hubs: ' ~ overwrite_hash_values, output_logs ) }}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql1 = datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') %}
        {% do run_query(overwrite_sql1) %}

        {% set overwrite_sql2 = datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) %}
        {% do run_query(overwrite_sql2) %}
        
        {% if drop_old_values == 'true' %}
            {# Dropping the deprecated column (Calls the translated macro) #}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}
    
{% endmacro %}


{% macro exasol__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set update_sql %}
    MERGE INTO {{ hub_relation }} hub
    USING (

        SELECT 
            hub.{{ hashkey }} as {{ hashkey }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ hub_relation }} hub  
        WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT 
            hub.{{ hashkey }} as {{ hashkey }},
            hub.{{ hashkey }} as {{ new_hashkey_name }}
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
    ) nh
    ON nh.{{ hashkey }} = hub.{{ hashkey }}
    WHEN MATCHED THEN
        UPDATE SET hub.{{ new_hashkey_name}} = nh.{{ new_hashkey_name}};
    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}