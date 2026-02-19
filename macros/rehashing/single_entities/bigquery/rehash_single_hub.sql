{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {% if overwrite_hash_values %}
        {% set new_hashkey_name = hashkey %}
    {% else %}
        {% set new_hashkey_name = hashkey + '_new' %}
    {% endif %}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition for new hashkey. #}
    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}

    {% set rename_sql = get_rename_table_sql(hub_relation, hub_relation.identifier ~ '_deprecated') %}
    {% do run_query(rename_sql) %}

    {# Get update SQL statement to calculate new hashkey. #}
    {% set create_sql = datavault4dbt.hub_update_statement(hub_relation=hub_relation,
                                                           new_hashkey_name=new_hashkey_name,
                                                           hashkey=hashkey,
                                                           hash_config_dict=hash_config_dict) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...' ~ create_sql, output_logs) }}
    {{ '/* CREATE STATEMENT FOR ' ~ hub ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}

    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}

    {% if drop_old_values %}
        {% set old_table_relation = make_temp_relation(hub_relation,suffix='_deprecated') %}

        {# Drop old Hub table and rename _rehashed Hub table to original Hub name. #}
        {{ log('Dropping old table: ' ~ old_table_relation, output_logs) }}
        {% do run_query(drop_table(old_table_relation)) %}
        {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}

    {% endif %}

    {{ return(columns_to_drop) }}
    
{% endmacro %}


{% macro bigquery__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set old_hashkey_name = hashkey + '_deprecated' %}
    {% set old_table_relation = make_temp_relation(hub_relation,suffix='_deprecated') %}

    
    {# Extract business keys from hash_config_dict, since business_key_list is not passed directly to the macro. #}
    {% set raw_keys = hash_config_dict.values() | list | first %}
    {% if raw_keys | length == 1 and ' ' in raw_keys[0] %}
        {% set business_key_list = raw_keys[0].split() %}
    {% else %}
        {% set business_key_list = raw_keys %}
    {% endif %}

    {% set create_sql %}
    CREATE TABLE {{ hub_relation }} AS (
        SELECT
            hub.{{ hashkey }} as {{ old_hashkey_name }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }},
            {{ datavault4dbt.print_list(business_key_list, src_alias='hub')}},
            hub.{{ ldts_alias }},
            hub.{{ rsrc_alias }}
        FROM {{ old_table_relation }} hub
        WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
        UNION ALL

        SELECT 
            hub.{{ hashkey }} as {{ old_hashkey_name }},
            hub.{{ hashkey }} as {{ new_hashkey_name }},
            {{ datavault4dbt.print_list(business_key_list, src_alias='hub')}},
            hub.{{ ldts_alias }},
            hub.{{ rsrc_alias }}
        FROM {{ old_table_relation }} hub
        WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    )
    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}