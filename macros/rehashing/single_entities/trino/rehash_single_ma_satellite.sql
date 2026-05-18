{#

Works for multi-active satellites.

Example usage:

dbt run-operation rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'

#}



{% macro trino__rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set ma_satellite_relation = ref(ma_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var(datavault4dbt.ldts_alias, 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set ns = namespace(parent_already_rehashed=false) %}

    {#
        If parent entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
        hashkey column needs to be used for joining, and the regular hashkey should be selected.
        Otherwise, the regular hashkey should be used for joining.
    #}
    {% set all_parent_columns = adapter.get_columns_in_relation(parent_relation) %}
    {% for column in all_parent_columns %}
        {% if column.name|lower == hashkey|lower + '_deprecated' %}
            {% set ns.parent_already_rehashed = true %}
            {{ log('Parent already hashed, using rehashed value for ' ~ ma_satellite_relation.name, false) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Ensuring ma_keys is a list. #}
    {% if ma_keys is iterable and ma_keys is not string %}
        {% set ma_keys = ma_keys %}
    {% else %}
        {% set ma_keys = [ma_keys] %}
    {% endif %}

    {% set prefixed_payload = datavault4dbt.prefix(columns=payload, prefix_str='sat').split(',') %}
    {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}
    {% set prefixed_hashkey = 'sat.' ~ hashkey %}

    {% set hash_config_dict = {
            new_hashkey_name: prefixed_business_keys,
            new_hashdiff_name: {
                "is_hashdiff": true,
                "columns": prefixed_payload
                }
            } %}

    {# Trino memory connector does not support UPDATE. Use CTAS + DROP + RENAME instead.
       Explicitly select existing columns (excluding stale _new columns from failed prior runs). #}
    {% set existing_columns = adapter.get_columns_in_relation(ma_satellite_relation) %}
    {% set clean_cols = [] %}
    {% for col in existing_columns %}
        {% if not col.name.lower().endswith('_new') and not col.name.lower().endswith('_deprecated') %}
            {% do clean_cols.append('outer_sat.' ~ col.name) %}
        {% endif %}
    {% endfor %}

    {% set temp_identifier = ma_satellite_relation.identifier ~ '_rehash_tmp' %}
    {% set temp_relation = api.Relation.create(
        database=ma_satellite_relation.database,
        schema=ma_satellite_relation.schema,
        identifier=temp_identifier
    ) %}

    {# Clean up any orphaned temp table from a previous failed run. #}
    {% set drop_tmp_sql %}DROP TABLE IF EXISTS {{ temp_relation }}{% endset %}
    {% do run_query(drop_tmp_sql) %}

    {# Step 1: CTAS — select clean original MA satellite columns plus computed new hash columns.
       The subquery groups by (hk, ldts) to produce one hashdiff per key+timestamp combination.
       The outer join expands back to all MA satellite rows (one per MA key value). #}
    {{ log('Executing CTAS statement for MA satellite ' ~ ma_satellite ~ '...', output_logs) }}
    {% set ctas_sql %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT
        {{ clean_cols | join(',\n        ') }},
        nh.{{ new_hashkey_name }},
        nh.{{ new_hashdiff_name }}
    FROM {{ ma_satellite_relation }} outer_sat
    JOIN (

        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }},

            {% if new_hashkey_name not in hash_config_dict.keys() %}
                {# Business keys not provided — inherit rehashed hashkey from parent entity. #}
                parent.{{ select_hashkey_col }} as {{ new_hashkey_name }},
            {% endif %}

            {{ datavault4dbt.hash_columns(columns=hash_config_dict, main_hashkey_column=prefixed_hashkey, multi_active_key=ma_keys) }}

        FROM {{ ma_satellite_relation }} sat
        LEFT JOIN (
            SELECT
                {{ join_hashkey_col }},
                {{ datavault4dbt.print_list(business_key_list) }}
            FROM {{ parent_relation }}
        ) parent
            ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        GROUP BY sat.{{ hashkey }},
                 sat.{{ ldts_col }},
                 {{ datavault4dbt.print_list(business_key_list, src_alias='parent') }}
                {% if new_hashkey_name not in hash_config_dict.keys() %}
                 , parent.{{ select_hashkey_col }}
                {% endif %}

        UNION ALL

        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            sat.{{ hashkey }} AS {{ new_hashkey_name }},
            sat.{{ new_hashdiff_name | replace('_new', '') }} AS {{ new_hashdiff_name }}
        FROM {{ ma_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    ) nh
        ON nh.{{ ldts_col }} = outer_sat.{{ ldts_col }}
        AND nh.{{ hashkey }} = outer_sat.{{ hashkey }}
    {% endset %}
    {% do run_query(ctas_sql) %}
    {{ log('CTAS completed!', output_logs) }}

    {# Step 2: Drop original table. #}
    {% set drop_sql %}DROP TABLE {{ ma_satellite_relation }}{% endset %}
    {% do run_query(drop_sql) %}

    {# Step 3: Rename temp table to the original table name. #}
    {% set rename_table_sql %}ALTER TABLE {{ temp_relation }} RENAME TO {{ ma_satellite_relation.identifier }}{% endset %}
    {% do run_query(rename_table_sql) %}
    {{ log('MA satellite rehash (CTAS-based) completed for ' ~ ma_satellite ~ '!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'},
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# Rename existing hash columns if overwrite is requested. #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Run each rename as a separate query — Trino does not support multi-statement execution. #}
        {% set rename1_sql = datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') %}
        {% do run_query(rename1_sql) %}

        {% set rename2_sql = datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') %}
        {% do run_query(rename2_sql) %}

        {% set rename3_sql = datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) %}
        {% do run_query(rename3_sql) %}

        {% set rename4_sql = datavault4dbt.custom_get_rename_column_sql(relation=ma_satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff) %}
        {% do run_query(rename4_sql) %}

        {% if drop_old_values %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=ma_satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro trino__ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}
    {# Trino does not support UPDATE. The rehash logic uses CTAS in trino__rehash_single_ma_satellite instead. #}
    {{ exceptions.raise_compiler_error("trino__ma_satellite_update_statement is not supported. Use trino__rehash_single_ma_satellite which uses CTAS-based rehashing.") }}
{% endmacro %}
