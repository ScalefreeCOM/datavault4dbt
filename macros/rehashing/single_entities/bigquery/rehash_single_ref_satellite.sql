{#
    Usage example:
    dbt run-operation rehash_single_ref_satellite --args '{ref_satellite: customer_ref_rs, refkey: RK_CUSTOMER_R, hashdiff: HD_CUSTOMER_RS, payload: [C_NAME, C_REGION], overwrite_hash_values: true}'
#}

{% macro bigquery__rehash_single_ref_satellite(ref_satellite, refkey, hashdiff, payload, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(ref_satellite) %}

    {% set ldts_col = var('datavault4dbt.ldts_alias', 'ldts') %}

    {% if overwrite_hash_values %}
        {% set new_hashdiff_name = hashdiff %}
    {% else %}
        {% set new_hashdiff_name = hashdiff + '_new' %}
    {% endif %}

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

    {# Rename to _deprecated, recovering safely if a previous run was interrupted. #}
    {% set old_table_relation = datavault4dbt.rehash_prepare_rename(satellite_relation, output_logs=output_logs) %}

    {# generating the CREATE statement that populates the new column. #}
    {% set create_sql = datavault4dbt.ref_satellite_update_statement(satellite_relation=satellite_relation,
                                                new_hashdiff_name=new_hashdiff_name,
                                                refkey=refkey_list,
                                                hashdiff=hashdiff,
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...', output_logs) }}
    {{ '/* CREATE STATEMENT FOR ' ~ ref_satellite ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('CREATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# old_table_relation set above by rehash_prepare_rename. #}
    {{ log('Dropping old table: ' ~ old_table_relation, output_logs) }}
    {% do run_query(drop_table(old_table_relation)) %}

    {% if drop_old_values %}
        {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro bigquery__ref_satellite_update_statement(satellite_relation, new_hashdiff_name, refkey, hashdiff, ldts_col, hash_config_dict) %}

    {% set old_table_relation = datavault4dbt.rehash_deprecated_relation(satellite_relation) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Extract all column names, excluding the hashdiff (selected explicitly with new value). #}
    {% set all_columns = adapter.get_columns_in_relation(old_table_relation) | map(attribute='name') | list %}

    {% if new_hashdiff_name not in all_columns %}
        {% set hashdiff_name = new_hashdiff_name | replace('_new', '') %}
        {% set old_hashdiff_name = hashdiff_name + '_deprecated' %}
        {% set exclude_columns = [hashdiff_name] %}
    {% else %}
        {% set hashdiff_name = new_hashdiff_name %}
        {% set old_hashdiff_name = new_hashdiff_name + '_deprecated' %}
        {% set exclude_columns = [new_hashdiff_name] %}
    {% endif %}

    {% set filtered_columns = [] %}
    {% for col in all_columns %}
        {% if col not in exclude_columns %}
            {% do filtered_columns.append('sat.' ~ col) %}
        {% endif %}
    {% endfor %}

    {% set select_clause = filtered_columns | join(',\n            ') %}

    {% set create_sql %}
    CREATE OR REPLACE TABLE {{ satellite_relation }} AS

        WITH calculate_hd_correctly AS (
            SELECT
                {% for key in refkey %}
                sat.{{ key }},
                {% endfor %}
                sat.{{ ldts_col }},
                {{ datavault4dbt.hash_columns(columns=hash_config_dict) }}
            FROM {{ old_table_relation }} sat
            WHERE sat.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        )
        SELECT
            hd.{{ new_hashdiff_name }},
            sat.{{ hashdiff_name }} AS {{ old_hashdiff_name }},
            {{ select_clause }}
        FROM calculate_hd_correctly hd
        LEFT JOIN {{ old_table_relation }} sat
            ON hd.{{ ldts_col }} = sat.{{ ldts_col }}
            {% for key in refkey %}
            AND hd.{{ key }} = sat.{{ key }}
            {% endfor %}

        UNION ALL

        SELECT
            sat.{{ hashdiff }} AS {{ new_hashdiff_name }},
            sat.{{ hashdiff }} AS {{ old_hashdiff_name }},
            {{ select_clause }}
        FROM {{ old_table_relation }} sat
        WHERE sat.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    {% endset %}

    {{ return(create_sql) }}

{% endmacro %}
