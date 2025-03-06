{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true) %}

    {% set hub_relation = ref(hub) %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# Alter existing Hub to add new hashkey column. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ alter_relation_add_remove_columns(relation=hub_relation, add_columns=new_hash_col) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition for new hashkey. #}
    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}

    {# Get update SQL statement to calculate new hashkey. #}
    {% set update_sql = datavault4dbt.hub_update_statement(hub_relation=hub_relation,
                                                           new_hashkey_name=new_hashkey_name,
                                                           hashkey=hashkey,
                                                           hash_config_dict=hash_config_dict) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
        {{ get_rename_column_sql(relation=hub_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') }}
        {{ get_rename_column_sql(relation=hub_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) }}
        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        {{ alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}
        
        {{ log('Existing Hash values overwritten!', output_logs) }}

    {% endif %}

    {{ return(columns_to_drop) }}
    
{% endmacro %}

{% macro hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {{ adapter.dispatch('hub_update_statement', 'datavault4dbt')(hub_relation=hub_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       hashkey=hashkey, 
                                                                       hash_config_dict=hash_config_dict) }}

{% endmacro %}

{% macro default__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set update_sql %}
    UPDATE {{ hub_relation }} hub
    SET 
        {{ new_hashkey_name}} = nh.{{ new_hashkey_name}}
    FROM (

        SELECT 
            hub.{{ hashkey }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ hub_relation }} hub            
    ) nh
    WHERE nh.{{ hashkey }} = hub.{{ hashkey }}
    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}