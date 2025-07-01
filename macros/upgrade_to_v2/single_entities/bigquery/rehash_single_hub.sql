{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {{ log('before_hub_relation: ' ~ hub_relation, info=True) }}
    {{ log('before_hub_relation.type: ' ~ hub_relation.is_table, info=True) }}

    {% set hub_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=(hub)) %}

    {{ log('hub_relation: ' ~ hub_relation, info=True) }}
    {{ log('hub_relation.type: ' ~ hub_relation.is_table, info=True) }}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

    {# Alter existing Hub to add new hashkey column. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ bigquery__alter_relation_add_columns(relation=hub_relation, add_columns=new_hash_col) }} 
                {# since alter is only used on tables not views you could hardcode like:  ...,relation_type='table' #}
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
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}]%}

    {{ log('Overwrite_hash_values for hubs: ' ~ overwrite_hash_values, true ) }}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', true) }}

        {% set rename_statements %}
            ALTER TABLE {{ hub_relation }} 
                RENAME COLUMN {{ hashkey }} TO {{ hashkey }}_deprecated;
            ALTER TABLE {{ hub_relation }} 
                RENAME COLUMN {{ new_hashkey_name }} TO {{ hashkey }};
        {% endset %}
        {% do run_query(rename_statements) %}
        
        {% if drop_old_values %}
            {{ bigquery__alter_relation_drop_columns(relation=hub_relation, drop_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}
    
{% endmacro %}


{% macro bigquery__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}
    {% set rsrc_col = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {{ log('unknown_value_rsrc : ' ~ unknown_value_rsrc, info=true) }}
    {{ log('error_value_rsrc : ' ~ error_value_rsrc, info=true) }}

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
    AND {{ rsrc_col }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}');

    UPDATE {{ hub_relation }}
    SET {{ new_hashkey_name }} = {{ hashkey }}
    WHERE {{ rsrc_col }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}