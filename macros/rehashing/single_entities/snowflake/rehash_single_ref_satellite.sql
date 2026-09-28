{#
    Usage example:
    dbt run-operation rehash_single_ref_satellite --args '{ref_satellite: customer_ref_rs, refkey: RK_CUSTOMER_R, hashdiff: HD_CUSTOMER_RS, payload: [C_NAME, C_REGION], overwrite_hash_values: true}'
#}

{% macro snowflake__rehash_single_ref_satellite(ref_satellite, refkey, hashdiff, payload, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set satellite_relation = ref(ref_satellite) %}

    {% set ldts_col = var('datavault4dbt.ldts_alias', 'ldts') %}

    {% set new_hashdiff_name = hashdiff + '_new' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}

    {# Create definition of new column for ALTER statement. #}
    {% set new_hash_columns = [
        {"name": new_hashdiff_name,
         "data_type": hash_datatype}
    ]%}

    {# ALTER existing ref satellite to add new hashdiff column. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=satellite_relation, add_columns=new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

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

    {# generating the UPDATE statement that populates the new column. #}
    {% set update_sql = datavault4dbt.ref_satellite_update_statement(satellite_relation=satellite_relation,
                                                new_hashdiff_name=new_hashdiff_name,
                                                refkey=refkey,
                                                hashdiff=hashdiff,
                                                ldts_col=ldts_col,
                                                hash_config_dict=hash_config_dict) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ ref_satellite ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {% set columns_to_drop = [
        {"name": hashdiff + '_deprecated'}
    ]%}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
        {{ datavault4dbt.custom_get_rename_column_sql(relation=satellite_relation, old_col_name=hashdiff, new_col_name=hashdiff + '_deprecated') }}
        {{ datavault4dbt.custom_get_rename_column_sql(relation=satellite_relation, old_col_name=new_hashdiff_name, new_col_name=hashdiff) }}
        {% endset %}
        {% do run_query(overwrite_sql) %}

        {% if drop_old_values %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=satellite_relation, remove_columns=columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}


{% macro snowflake__ref_satellite_update_statement(satellite_relation, new_hashdiff_name, refkey, hashdiff, ldts_col, hash_config_dict) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {# Ensuring refkey is a list to support composite reference keys. #}
    {% if refkey is iterable and refkey is not string %}
        {% set refkey_list = refkey %}
    {% else %}
        {% set refkey_list = [refkey] %}
    {% endif %}

    {% set update_sql %}
    UPDATE {{ satellite_relation }} sat
    SET
        {{ new_hashdiff_name }} = nh.{{ new_hashdiff_name }}
    FROM (

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
    WHERE nh.{{ ldts_col }} = sat.{{ ldts_col }}
    {% for key in refkey_list %}
    AND nh.{{ key }} = sat.{{ key }}
    {% endfor %}

    {% endset %}

    {{ return(update_sql) }}

{% endmacro %}
