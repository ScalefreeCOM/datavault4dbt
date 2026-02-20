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

{% macro bigquery__rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set nh_satellite_relation = ref(nh_satellite) %}
    
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% if overwrite_hash_values %}
        {% set new_hashkey_name = hashkey %}
    {% else %}
        {% set new_hashkey_name = hashkey + '_new' %}
    {% endif %}

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


    {% set rename_sql = get_rename_table_sql(nh_satellite_relation, nh_satellite_relation.identifier ~ '_deprecated') %}
    {% do run_query(rename_sql) %}

    {# generating the CREATE statement that populates the new columns. #}
    {% set create_sql = datavault4dbt.nh_satellite_update_statement(nh_satellite_relation=nh_satellite_relation,
                                                new_hashkey_name=new_hashkey_name,
                                                hashkey=hashkey, 
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict,
                                                parent_relation=parent_relation) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...', output_logs) }}
    {{ '/* CREATE STATEMENT FOR ' ~ nh_satellite ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'}
    ]%}

    {% if drop_old_values %}
        {% set old_table_relation = make_temp_relation(nh_satellite_relation,suffix='_deprecated') %}

        {{ log('Dropping old table: ' ~ old_table_relation, output_logs) }}
        {% do run_query(drop_table(old_table_relation)) %}

        {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=nh_satellite_relation, remove_columns=columns_to_drop) }}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro bigquery__nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}

    {% set ns = namespace(parent_already_rehashed=false) %}
    
    {% set old_hashkey_name = hashkey + '_deprecated' %}
    {% set old_table_relation = make_temp_relation(nh_satellite_relation,suffix='_deprecated') %}
    
    {#
        If parent entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
        hashkey column needs to be used for joining, and the regular hashkey should be selected. 

        Otherwise, the regular hashkey should be used for joining. 
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {{ log('parent column names: ' ~ all_parent_columns, false) }}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('parent_already hashed set to true for ' ~ nh_satellite_relation.name, false) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = hashkey %}
    {% endif %}
    
    {# Filter out the excluded columns by name #}
    {% set all_columns = adapter.get_columns_in_relation(old_table_relation) %}
    {% set exclude_columns = [new_hashkey_name] %}
    {% set filtered_columns = [] %}
    {% for col in all_columns %}
        {% if col.name not in exclude_columns %}
            {% do filtered_columns.append(col) %}
        {% endif %}
    {% endfor %}

    {# Extract only the column names #}
    {% set selected_column_names = filtered_columns | map(attribute='name') | list %}
    {% set select_clause = selected_column_names | join(', ') %}
    {{ log('SELECT clause: ' ~ select_clause, false) }}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {{ log('hash_config_dict' ~ hash_config_dict, false) }}

    {% set create_sql %}
    CREATE OR REPLACE TABLE {{ nh_satellite_relation }} AS

        WITH calculate_hashes_correctly AS (
            SELECT
                src.{{ hashkey }} AS original_hashkey,
                src.{{ ldts_col }} AS original_ldts,
                src.{{ rsrc_alias }} AS original_rsrc,
                {% if datavault4dbt.is_something(hash_config_dict) %}
                    {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
                {% else %}
                    parent.{{ select_hashkey_col }} as {{ new_hashkey_name }}
                {% endif %}
            FROM {{ old_table_relation }} src
            LEFT JOIN {{ parent_relation }} parent
                ON src.{{ hashkey }} = parent.{{ join_hashkey_col }}
            WHERE src.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
                {# GROUP BY ALL #}
            )    
            SELECT 
                ha.original_hashkey as {{ old_hashkey_name }},
                ha.{{ new_hashkey_name }},
                sat.{{ select_clause }}
            FROM calculate_hashes_correctly ha
            LEFT JOIN {{ old_table_relation }} sat
                ON ha.original_hashkey = sat.{{ hashkey }}
                AND ha.original_ldts = sat.{{ ldts_col }}

            UNION ALL

            SELECT 
                sat.{{ hashkey }} as {{ old_hashkey_name }},
                sat.{{ hashkey }} as {{ new_hashkey_name }},
                sat.{{ select_clause }}
            FROM {{ old_table_relation }} sat
            WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}