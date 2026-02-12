{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=(hub)) %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition for new hashkey. #}
    {% set hash_config_dict = {hashkey: business_key_list} %}

    {# Get update SQL statement to calculate new hashkey. #}
    {% set create_sql = datavault4dbt.hub_update_statement(hub_relation=hub_relation,
                                                           new_hashkey_name=hashkey,
                                                           hashkey=hashkey,
                                                           hash_config_dict=hash_config_dict) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...' ~ create_sql, output_logs) }}
    {{ '/* CREATE STATEMENT FOR ' ~ hub ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}

    {{ log('Renaming old hash columns for hubs: ' ~ overwrite_hash_values, output_logs) }}

    {# Renaming existing hash columns. #}
    {% if overwrite_hash_values %}
        {{ log('Renaming existing hash columns...', output_logs) }}
        
        {% set rename_statement %}
            ALTER TABLE {{ hub_relation }} 
                RENAME COLUMN {{ hashkey }} TO {{ hashkey }}_deprecated;
        {% endset %}
        {% do run_query(rename_statement) %}

    {% endif %}
            
    {% if drop_old_values %}
        {# Drop old Hub table and rename _rehashed Hub table to original Hub name. #}
        {% set old_table_name = hub_relation %}
        {% set new_table_name = hub_relation.database ~ '.' ~ hub_relation.schema ~ '.' ~ hub_relation.identifier ~ '_rehashed' %}

        {{ log('Dropping old table: ' ~ old_table_name, output_logs) }}
        {% do run_query(bigquery__drop_table(old_table_name)) %}

        {% set rename_sql = bigquery__get_rename_table_sql(new_table_name, hub_relation.identifier) %}
        {{ log('Renaming rehashed Hub to original Hub name: ' ~ rename_sql, output_logs) }}
        {% do run_query(rename_sql) %}
    {% endif %}
    
{% endmacro %}


{% macro bigquery__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}
    
    {# Extract business keys from hash_config_dict, since business_key_list is not passed directly to the macro. #}
    {% set raw_keys = hash_config_dict.values() | list | first %}
    {% if raw_keys | length == 1 and ' ' in raw_keys[0] %}
        {% set business_key_list = raw_keys[0].split() %}
    {% else %}
        {% set business_key_list = raw_keys %}
    {% endif %}

    {% set create_sql %}
    CREATE TABLE {{ hub_relation.database }}.{{ hub_relation.schema }}.{{hub_relation.identifier ~ '_rehashed'}} AS (
        SELECT
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }},
            {{ datavault4dbt.print_list(business_key_list, src_alias='hub')}},
            hub.{{ ldts_alias }},
            hub.{{ rsrc_alias }}
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
        UNION ALL

        SELECT 
            * 
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    )
    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}