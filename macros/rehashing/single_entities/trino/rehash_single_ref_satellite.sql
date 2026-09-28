{#
    Usage example:
    dbt run-operation rehash_single_ref_satellite --args '{ref_satellite: customer_ref_rs, refkey: RK_CUSTOMER_R, hashdiff: HD_CUSTOMER_RS, payload: [C_NAME, C_REGION], overwrite_hash_values: true}'
#}

{% macro trino__rehash_single_ref_satellite(ref_satellite, refkey, hashdiff, payload, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(ref_satellite) %}

    {% set ldts_col = var('datavault4dbt.ldts_alias', 'ldts') %}

    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Ensuring refkey is a list to support composite reference keys. #}
    {% if refkey is iterable and refkey is not string %}
        {% set refkey_list = refkey %}
    {% else %}
        {% set refkey_list = [refkey] %}
    {% endif %}

    {# Ensuring payload is a list. #}
    {% if payload is iterable and payload is not string %}
        {% set payload_list = payload %}
    {% else %}
        {% set payload_list = [payload] %}
    {% endif %}

    {# Adding prefixes to column names for proper selection. #}
    {% set prefixed_payload = datavault4dbt.prefix(columns=payload_list, prefix_str='sat').split(',') %}

    {% set hash_config_dict = {
        new_hashdiff_name: {
            "is_hashdiff": true,
            "columns": prefixed_payload
        }
    } %}

    {# Trino does not support UPDATE. Use CTAS + DROP + RENAME instead.
       Explicitly select existing columns (excluding stale _new columns from failed prior runs). #}
    {% set existing_columns = adapter.get_columns_in_relation(satellite_relation) %}
    {% set clean_cols = [] %}
    {% for col in existing_columns %}
        {% if not col.name.lower().endswith('_new') and not col.name.lower().endswith('_deprecated') %}
            {% do clean_cols.append('outer_sat.' ~ col.name) %}
        {% endif %}
    {% endfor %}

    {% set temp_identifier = satellite_relation.identifier ~ '_rehash_tmp' %}
    {% set temp_relation = api.Relation.create(
        database=satellite_relation.database,
        schema=satellite_relation.schema,
        identifier=temp_identifier
    ) %}

    {# Clean up any orphaned temp table from a previous failed run. #}
    {% set drop_tmp_sql %}DROP TABLE IF EXISTS {{ temp_relation }}{% endset %}
    {% do run_query(drop_tmp_sql) %}

    {# Step 1: CTAS — select clean original ref satellite columns plus computed new hashdiff. #}
    {{ log('Executing CTAS statement for ref satellite ' ~ ref_satellite ~ '...', output_logs) }}
    {% set ctas_sql %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT
        {{ clean_cols | join(',\n        ') }},
        nh.{{ new_hashdiff_name }}
    FROM {{ satellite_relation }} outer_sat
    JOIN (

        SELECT
            {% for key in refkey_list %}
            sat.{{ key }},
            {% endfor %}
            sat.{{ ldts_col }},
            {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
        FROM {{ satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT
            {% for key in refkey_list %}
            sat.{{ key }},
            {% endfor %}
            sat.{{ ldts_col }},
            sat.{{ hashdiff }} AS {{ new_hashdiff_name }}
        FROM {{ satellite_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    ) nh
        ON nh.{{ ldts_col }} = outer_sat.{{ ldts_col }}
        {% for key in refkey_list %}
        AND nh.{{ key }} = outer_sat.{{ key }}
        {% endfor %}
    {% endset %}
    {% do run_query(ctas_sql) %}
    {{ log('CTAS completed!', output_logs) }}

    {# Step 2: Drop original table. #}
    {% set drop_sql %}DROP TABLE {{ satellite_relation }}{% endset %}
    {% do run_query(drop_sql) %}

    {# Step 3: Rename temp table to the original table name. #}
    {% set rename_table_sql %}ALTER TABLE {{ temp_relation }} RENAME TO {{ satellite_relation.identifier }}{% endset %}
    {% do run_query(rename_table_sql) %}
    {{ log('Ref satellite rehash (CTAS-based) completed for ' ~ ref_satellite ~ '!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# Rename existing hash columns if overwrite is requested. #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Run each rename as a separate query — Trino does not support multi-statement execution. #}
        {% set rename1_sql = datavault4dbt.custom_get_rename_column_sql(relation=satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') %}
        {% do run_query(rename1_sql) %}

        {% set rename2_sql = datavault4dbt.custom_get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff) %}
        {% do run_query(rename2_sql) %}

        {% if drop_old_values %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro trino__ref_satellite_update_statement(satellite_relation, new_hashdiff_name, refkey, hashdiff, ldts_col, hash_config_dict) %}
    {# Trino does not support UPDATE. The rehash logic uses CTAS in trino__rehash_single_ref_satellite instead. #}
    {{ exceptions.raise_compiler_error("trino__ref_satellite_update_statement is not supported. Use trino__rehash_single_ref_satellite which uses CTAS-based rehashing.") }}
{% endmacro %}
