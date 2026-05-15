{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro trino__rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set hub_relation = ref(hub) %}

    {% set new_hashkey_name = hashkey + '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR(32)') %}

    {# Ensuring business_keys is a list. #}
    {% if business_keys is iterable and business_keys is not string %}
        {% set business_key_list = business_keys %}
    {% else %}
        {% set business_key_list = [business_keys] %}
    {% endif %}

    {# Set Hash definition for new hashkey. #}
    {% set hash_config_dict = {new_hashkey_name: business_key_list} %}

    {# Generate hash expression and strip trailing ' AS alias' for use in CTAS SELECT. #}
    {%- set hash_expr_full = datavault4dbt.hash_columns(columns=hash_config_dict) | trim -%}
    {%- set hash_expr_only = hash_expr_full.rsplit(' AS ', 1)[0] -%}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Trino memory connector does not support UPDATE. Use CTAS + DROP + RENAME instead.
       Explicitly select existing columns (excluding stale _new columns from failed prior runs). #}
    {% set existing_columns = adapter.get_columns_in_relation(hub_relation) %}
    {% set clean_cols = [] %}
    {% for col in existing_columns %}
        {% if not col.name.lower().endswith('_new') and not col.name.lower().endswith('_deprecated') %}
            {% do clean_cols.append(col.name) %}
        {% endif %}
    {% endfor %}

    {% set temp_identifier = hub_relation.identifier ~ '_rehash_tmp' %}
    {% set temp_relation = api.Relation.create(
        database=hub_relation.database,
        schema=hub_relation.schema,
        identifier=temp_identifier
    ) %}

    {# Clean up any orphaned temp table from a previous failed run. #}
    {% set drop_tmp_sql %}DROP TABLE IF EXISTS {{ temp_relation }}{% endset %}
    {% do run_query(drop_tmp_sql) %}

    {# Step 1: CTAS — create temp table with clean original columns plus computed new hashkey. #}
    {{ log('Executing CTAS statement for hub ' ~ hub ~ '...', output_logs) }}
    {% set ctas_sql %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT
        {{ clean_cols | join(',\n        ') }},
        CASE
            WHEN {{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
            THEN {{ hash_expr_only }}
            ELSE {{ hashkey }}
        END AS {{ new_hashkey_name }}
    FROM {{ hub_relation }}
    {% endset %}
    {% do run_query(ctas_sql) %}
    {{ log('CTAS completed!', output_logs) }}

    {# Step 2: Drop original table. #}
    {% set drop_sql %}DROP TABLE {{ hub_relation }}{% endset %}
    {% do run_query(drop_sql) %}

    {# Step 3: Rename temp table to the original table name. #}
    {% set rename_table_sql %}ALTER TABLE {{ temp_relation }} RENAME TO {{ hub_relation.identifier }}{% endset %}
    {% do run_query(rename_table_sql) %}
    {{ log('Hub rehash (CTAS-based) completed for ' ~ hub ~ '!', output_logs) }}

    {% set columns_to_drop = [{"name": hashkey + '_deprecated'}] %}

    {{ log('Overwrite_hash_values for hubs: ' ~ overwrite_hash_values, output_logs ) }}

    {# Rename existing hash columns if overwrite is requested. #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Run each rename as a separate query — Trino does not support multi-statement execution. #}
        {% set rename1_sql = datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=hashkey, new_col_name=hashkey + '_deprecated') %}
        {% do run_query(rename1_sql) %}

        {% set rename2_sql = datavault4dbt.custom_get_rename_column_sql(relation=hub_relation, old_col_name=new_hashkey_name, new_col_name=hashkey) %}
        {% do run_query(rename2_sql) %}

        {% if drop_old_values == 'true' %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=hub_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro trino__hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}
    {# Trino does not support UPDATE. The rehash logic uses CTAS in trino__rehash_single_hub instead. #}
    {{ exceptions.raise_compiler_error("trino__hub_update_statement is not supported. Use trino__rehash_single_hub which uses CTAS-based rehashing.") }}
{% endmacro %}
