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

{% macro trino__rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set nh_satellite_relation = ref(nh_satellite) %}
    {% set parent_relation = ref(parent_entity) %}

    {% set ldts_col = var('datavault4dbt.ldts_alias', 'ldts') %}

    {% set new_hashkey_name = hashkey + '_new' %}

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
            {{ log('Parent already hashed, using rehashed value for ' ~ nh_satellite_relation.name, false) }}
        {% endif %}
    {% endfor %}

    {% if ns.parent_already_rehashed %}
        {% set join_hashkey_col = hashkey + '_deprecated' %}
        {% set select_hashkey_col = hashkey %}
    {% else %}
        {% set join_hashkey_col = hashkey %}
        {% set select_hashkey_col = new_hashkey_name %}
    {% endif %}

    {% if datavault4dbt.is_something(business_keys) %}
        {% if business_keys is iterable and business_keys is not string %}
            {% set business_key_list = business_keys %}
        {% else %}
            {% set business_key_list = [business_keys] %}
        {% endif %}

        {% set prefixed_business_keys = datavault4dbt.prefix(columns=business_key_list, prefix_str='parent').split(',') %}
        {% set hash_config_dict = {new_hashkey_name: prefixed_business_keys} %}

    {% else %}

        {% set hash_config_dict = none %}

    {% endif %}

    {# Trino memory connector does not support UPDATE. Use CTAS + DROP + RENAME instead.
       Explicitly select existing columns (excluding stale _new columns from failed prior runs). #}
    {% set existing_columns = adapter.get_columns_in_relation(nh_satellite_relation) %}
    {% set clean_cols = [] %}
    {% for col in existing_columns %}
        {% if not col.name.lower().endswith('_new') and not col.name.lower().endswith('_deprecated') %}
            {% do clean_cols.append('outer_sat.' ~ col.name) %}
        {% endif %}
    {% endfor %}

    {% set temp_identifier = nh_satellite_relation.identifier ~ '_rehash_tmp' %}
    {% set temp_relation = api.Relation.create(
        database=nh_satellite_relation.database,
        schema=nh_satellite_relation.schema,
        identifier=temp_identifier
    ) %}

    {# Clean up any orphaned temp table from a previous failed run. #}
    {% set drop_tmp_sql %}DROP TABLE IF EXISTS {{ temp_relation }}{% endset %}
    {% do run_query(drop_tmp_sql) %}

    {# Step 1: CTAS — select clean original NH satellite columns plus computed new hashkey column. #}
    {{ log('Executing CTAS statement for NH satellite ' ~ nh_satellite ~ '...', output_logs) }}
    {% set ctas_sql %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT
        {{ clean_cols | join(',\n        ') }},
        nh.{{ new_hashkey_name }}
    FROM {{ nh_satellite_relation }} outer_sat
    JOIN (

        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            {% if hash_config_dict is not none and hash_config_dict is defined and hash_config_dict %}
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
            {% else %}
                parent.{{ select_hashkey_col }} as {{ new_hashkey_name }}
            {% endif %}
        FROM {{ nh_satellite_relation }} sat
        LEFT JOIN {{ parent_relation }} parent
            ON sat.{{ hashkey }} = parent.{{ join_hashkey_col }}
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT
            sat.{{ hashkey }},
            sat.{{ ldts_col }},
            sat.{{ hashkey }} AS {{ new_hashkey_name }}
        FROM {{ nh_satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    ) nh
        ON nh.{{ ldts_col }} = outer_sat.{{ ldts_col }}
        AND nh.{{ hashkey }} = outer_sat.{{ hashkey }}
    {% endset %}
    {% do run_query(ctas_sql) %}
    {{ log('CTAS completed!', output_logs) }}

    {# Step 2: Drop original table. #}
    {% set drop_sql %}DROP TABLE {{ nh_satellite_relation }}{% endset %}
    {% do run_query(drop_sql) %}

    {# Step 3: Rename temp table to the original table name. #}
    {% set rename_table_sql %}ALTER TABLE {{ temp_relation }} RENAME TO {{ nh_satellite_relation.identifier }}{% endset %}
    {% do run_query(rename_table_sql) %}
    {{ log('NH satellite rehash (CTAS-based) completed for ' ~ nh_satellite ~ '!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashkey + '_deprecated'}
    ]%}

    {# Rename existing hash columns if overwrite is requested. #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Run each rename as a separate query — Trino does not support multi-statement execution. #}
        {% set rename1_sql = datavault4dbt.custom_get_rename_column_sql(relation=nh_satellite_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') %}
        {% do run_query(rename1_sql) %}

        {% set rename2_sql = datavault4dbt.custom_get_rename_column_sql(relation=nh_satellite_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) %}
        {% do run_query(rename2_sql) %}

        {% if drop_old_values %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=nh_satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro trino__nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}
    {# Trino does not support UPDATE. The rehash logic uses CTAS in trino__rehash_single_nh_satellite instead. #}
    {{ exceptions.raise_compiler_error("trino__nh_satellite_update_statement is not supported. Use trino__rehash_single_nh_satellite which uses CTAS-based rehashing.") }}
{% endmacro %}
