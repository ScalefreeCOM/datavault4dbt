{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro fabric__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set old_hashkey_name = hashkey + '_deprecated' %} 

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARBINARY(8000)') %}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {% set deprecated_hash_col = [{"name": old_hashkey_name, "data_type": hash_datatype}] %}

    {# Alter existing Hub to add deprecated hashkey column. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=hub_relation, add_columns=deprecated_hash_col) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# Update SQL statement to copy hashkey to _depr column  #}
    {% set depr_update_sql %}
        UPDATE {{ hub_relation }}
        SET 
            {{ old_hashkey_name }} = {{ hashkey }};
    {% endset %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ depr_update_sql ~ '*/' }}
    {% do run_query(depr_update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% if overwrite_hash_values %}
        {% set new_hashkey_name = hashkey %}
        {% set hash_config_dict = {new_hashkey_name: business_key_list} %}
    {% else %}
        {% set hash_config_dict = {new_hashkey_name: business_key_list} %}
        {# Alter existing Hub to add new hashkey column. #}
        {% set new_hash_col = [{"name": new_hashkey_name, "data_type": hash_datatype}] %}

        {{ log('Executing ALTER TABLE statement...', output_logs) }}
        {{ datavault4dbt.alter_relation_add_remove_columns(relation=hub_relation, add_columns=new_hash_col) }}
        {{ log('ALTER TABLE statement completed!', output_logs) }}
    {% endif %}

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

    {# Deleting old hashkey #}
    {% if drop_old_values or not overwrite_hash_values %}
        {{ datavault4dbt.alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}
        {{ log('Deprecated hashkey column removed!', output_logs) }}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}



{% macro fabric__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {% set old_hashkey_name = hashkey + '_deprecated' %} 

    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set update_sql %}
    UPDATE {{ hub_relation }}
    SET 
        {{ new_hashkey_name}} = nh.{{ new_hashkey_name}}
    FROM (
        SELECT 
            hub.{{ old_hashkey_name }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT 
            hub.{{ old_hashkey_name }},
            hub.{{ hashkey }} as {{ new_hashkey_name }}
        FROM {{ hub_relation }} hub
        WHERE hub.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
    ) nh
    WHERE nh.{{ old_hashkey_name }} = {{ hub_relation }}.{{ old_hashkey_name }}
    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}