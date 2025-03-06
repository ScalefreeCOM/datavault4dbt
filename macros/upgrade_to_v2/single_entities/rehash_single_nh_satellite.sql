{#

    Works for Non-Historized Satellites either attached to Hubs or (NH)Links. 
    If attached to Hub: 
        Define Business Keys of Hub 
        OR Rehash Hub first, without overwriting hash values.

    If attached to (NH)Link:
        Rehash (NH)Link first, without overwriting hash values.

    Usage example:
    dbt run-operation rehash_single_nh_satellite --args '{nh_satellite: order_customer_n_ns, hashkey: HK_ORDER_CUSTOMER_NL, parent_entity: order_customer_nl, overwrite_hash_values: true}'
#}

{% macro rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true) %}

    {% set nh_satellite_relation = ref(nh_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add new hashkey and new hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ alter_relation_add_remove_columns(relation=nh_satellite_relation, add_columns=new_hash_columns) }}
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


    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql = datavault4dbt.nh_satellite_update_statement(nh_satellite_relation=nh_satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                hashkey=hashkey, 
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'}
    ]%}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
        {{ get_rename_column_sql(relation=nh_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') }}
        {{ get_rename_column_sql(relation=nh_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) }}
        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        {{ alter_relation_add_remove_columns(relation=nh_satellite_relation, remove_columns=columns_to_drop) }}
        
        {{ log('Existing Hash values overwritten!', output_logs) }}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}

    {{ adapter.dispatch('nh_satellite_update_statement', 'datavault4dbt')(nh_satellite_relation=nh_satellite_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       hashkey=hashkey, 
                                                                       ldts_col=ldts_col,
                                                                       hash_config_dict=hash_config_dict,
                                                                       parent_relation=parent_relation) }}

{% endmacro %}


{% macro default__nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}

    {% set update_sql %}
    UPDATE {{ nh_satellite_relation }} sat
    SET 
        {{ new_hashkey_name}} = nh.{{ new_hashkey_name}}
    FROM (

        SELECT 
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            {% if datavault4dbt.is_something(hash_config_dict) %}
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
            {% else %}
                parent.{{ new_hashkey_name }}
            {% endif %}
        FROM {{ nh_satellite_relation }} sat
        LEFT JOIN {{ parent_relation }} parent
            ON sat.{{ hashkey }} = parent.{{ hashkey }}
            
    ) nh
    WHERE nh.{{ ldts_col }} = sat.{{ ldts_col }}
    AND nh.{{ hashkey }} = sat.{{ hashkey }}
    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}