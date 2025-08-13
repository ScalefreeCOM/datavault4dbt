{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro fabric__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}
    {% set old_hashkey = hashkey + '_deprecated' %} 

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {% set temp_relation = make_temp_relation(hub_relation, suffix='__rehash_tmp')%}
    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}
    
    {# Set Hash definition for new hashkey. #}

    {% if overwrite_hash_values %}
        {% set hash_config_dict = {hashkey: business_key_list} %}
        {% set new_hashkey_name = hashkey %}
    {% else %}
        {% set hash_config_dict = {new_hashkey_name: business_key_list} %}
            {% if drop_old_values %}
                {% set old_hashkey = hashkey %} 
            {% endif %}   
    {% endif %}


    {% set create_sql %}

        CREATE TABLE {{ temp_relation }} AS (
            SELECT
                hub.{{ hashkey }} as {{ old_hashkey }},
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }},
                {{ datavault4dbt.print_list(business_key_list, src_alias='hub')}},
                hub.{{ ldts_alias }},
                hub.{{ rsrc_alias }}
            FROM {{ hub_relation }} hub
            WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
            
            UNION ALL

            SELECT 
                hub.{{ hashkey }} as {{ old_hashkey }},
                hub.{{ hashkey }} as {{ new_hashkey_name }},
                {{ datavault4dbt.print_list(business_key_list, src_alias='hub')}},
                hub.{{ ldts_alias }},
                hub.{{ rsrc_alias }} 
            FROM {{ hub_relation }} hub
            WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        )
    {% endset %}

    {{ log('Executing CREATE statement...', output_logs) }}
    {{ '/* CREATE STATEMENT FOR ' ~ hub ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}
        
    {# Deleting old hashkey #}
    {% if drop_old_values %}
        {{ alter_relation_add_remove_columns(relation=temp_relation, remove_columns=columns_to_drop) }}
        {{ log('Existing Hash values overwritten!', true) }}
    {% endif %}

    {{ fabric_delete_table(hub) }}

    {{ rename_relation(temp_relation, hub_relation)}}
    
    {{ return(columns_to_drop) }}

{% endmacro %}

{% macro fabric__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set update_sql %}
    UPDATE {{ hub_relation }}
    SET 
        {{ new_hashkey_name}} = nh.{{ new_hashkey_name}}
    FROM (
        SELECT 
            hub.{{ hashkey }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ hub_relation }} hub            
    ) nh
    WHERE nh.{{ hashkey }} = {{ hub_relation }}.{{ hashkey }}
    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}