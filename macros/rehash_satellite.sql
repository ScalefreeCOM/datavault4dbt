{#
    Usage example:
    dbt run-operation rehash_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro rehash_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys, overwrite_hash_values=false) %}

    {% set satellite_relation = ref(satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Create definition of new columns for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashkey_name,
         "data_type": hash_datatype},
        {"name": new_hashdiff_name, 
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add new hashkey and new hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', true) }}
    {{ alter_relation_add_remove_columns(relation=satellite_relation, add_columns=new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', true) }}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Adding prefixes to column names for proper selection. #}
    {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
    {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}

    {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys, new_hashdiff_name: prefixed_payload} %}


    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql %}
    UPDATE {{ satellite_relation }} sat
    SET 
        {{ new_hashkey_name}} = nh.{{ new_hashkey_name}},
        {{ new_hashdiff_name}} = nh.{{ new_hashdiff_name }}
    FROM (

        SELECT 
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ satellite_relation }} sat
        LEFT JOIN {{ parent_relation }} parent
            ON sat.{{ hashkey }} = parent.{{ hashkey }}
            
    ) nh
    WHERE nh.{{ ldts_col }} = sat.{{ ldts_col }}
    AND nh.{{ hashkey }} = sat.{{ hashkey }}
    {% endset %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', true) }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', true) }}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', true) }}

        {% set overwrite_sql %}
        {{ get_rename_column_sql(relation=satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') }}
        {{ get_rename_column_sql(relation=satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') }}
        {{ get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) }}
        {{ get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff ) }}
        {% endset %}

        {% do run_query(overwrite_sql) %}

        {% set columns_to_drop = [
            {"name": hashkey + '_deprecated'},
            {"name": hashdiff + '_deprecated'}
        ]%}
        
        {{ alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}
        
        {{ log('Existing Hash values overwritten!', true) }}

    {% endif %}


{% endmacro %}