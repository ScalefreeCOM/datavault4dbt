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

{% macro fabric__rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set nh_satellite_relation = ref(nh_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}

    {% set old_hashkey_name = hashkey + '_deprecated' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARBINARY(8000)') %}

    {# Create definition of deprecated columns for ALTER statement. #}
    {% set old_hash_columns = [
        {"name": old_hashkey_name,
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing satellite to add deprecated hashkey and deprecated hashdiff. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, add_columns=old_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {# Update SQL statement to copy hashkey to _depr column  #}
    {% set depr_update_sql %}
        UPDATE {{ nh_satellite_relation }}
        SET 
            {{ old_hashkey_name }} = {{ hashkey }};

    {% endset %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ depr_update_sql ~ '*/' }}
    {% do run_query(depr_update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}


    {% if datavault4dbt.is_something(business_keys) %}
        {# Ensuring business_keys is a list. #}
        {% if business_keys is iterable and business_keys is not string %}
            {% set business_key_list = business_keys %}
        {% else %}
            {% set business_key_list = [business_keys] %}
        {% endif %}

        {# Adding prefixes to column names for proper selection. #}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}

        {% if overwrite_hash_values %}

            {% set new_hashkey_name = hashkey %}
            {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys} %}
            
        {% else %}

            {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys} %}        
            
            {% set new_hash_columns = [
                {"name": new_hashkey_name,
                "data_type": hash_datatype}
            ]%}

            {# ALTER existing satellite to add new hashkey. #}
            {{ log('Executing ALTER TABLE statement...', output_logs) }}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, add_columns=new_hash_columns) }}
            {{ log('ALTER TABLE statement completed!', output_logs) }}

        {% endif %}
    {% else %}

        {% set hash_config_dict = none %}
        
        {% if overwrite_hash_values %}
            
            {% set new_hashkey_name = hashkey %}
            
        {% else %}

            {% set new_hash_columns = [
                {"name": new_hashkey_name,
                "data_type": hash_datatype}
            ]%}

            {# ALTER existing satellite to add new hashkey. #}
            {{ log('Executing ALTER TABLE statement...', output_logs) }}
            {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, add_columns=new_hash_columns) }}
            {{ log('ALTER TABLE statement completed!', output_logs) }}

        {% endif %}
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
    {{ '/* UPDATE STATEMENT FOR ' ~ nh_satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [{"name": old_hashkey_name}] %}

    {# Deleting old hashkey #}
    {% if drop_old_values or not overwrite_hash_values %}
        {{ datavault4dbt.alter_relation_add_remove_columns(relation=nh_satellite_relation, remove_columns=columns_to_drop) }}
        {{ log('Deprecated hashkey column removed!', output_logs) }}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro fabric__nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}
    
    {% set ns = namespace(parent_already_rehashed=false) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set old_hashkey_name = hashkey + '_deprecated' %}

    {#
        If parent entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
        hashkey column needs to be used for joining, and the regular hashkey should be selected. 

        Otherwise, the regular hashkey should be used for joining. 
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('parent_already hashed set to true for ' ~ nh_satellite_relation.name, output_logs) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = old_hashkey_name %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}

    {% set update_sql %}
        
        UPDATE {{ nh_satellite_relation }}
        SET 
            {{ new_hashkey_name}} = nh.{{ new_hashkey_name}} 
        FROM (
            SELECT 
                sat.{{ old_hashkey_name }},
                sat.{{ ldts_col }},
                {% if datavault4dbt.is_something(hash_config_dict) %}
                    {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
                {% else %}
                    parent.{{ select_hashkey_col }} AS {{ new_hashkey_name }}
                {% endif %}
            FROM {{ nh_satellite_relation }} sat
            LEFT JOIN {{ parent_relation }} parent
                ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
            WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

            UNION ALL

            SELECT
                sat.{{ old_hashkey_name }},
                sat.{{ ldts_col }},
                sat.{{ old_hashkey_name }} AS {{ new_hashkey_name }}
            FROM {{ nh_satellite_relation }} sat
            WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        ) nh
        WHERE nh.{{ ldts_col }} = {{ nh_satellite_relation }}.{{ ldts_col }}
        AND nh.{{ old_hashkey_name }} = {{ nh_satellite_relation }}.{{ old_hashkey_name }}

    {% endset %}
   
    {{ return(update_sql) }}

{% endmacro %}