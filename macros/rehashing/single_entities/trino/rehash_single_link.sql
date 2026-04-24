{#

Works for standard links and non-historized links.

Example usage:

dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'

#}



{% macro trino__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARCHAR(32)') %}
    {% set ns = namespace(
        hub_hashkeys=[],
        columns_to_drop=[{"name": link_hashkey + '_deprecated'}],
        hash_config_dict={},
        link_hashkey_input_cols=[]
    ) %}

    {% set link_relation = ref(link) %}

    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}
    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% set new_hub_hashkey_name = hub.hub_hashkey ~ '_new' %}

        {# Check if this hub has already been rehashed (has a _deprecated hashkey column). #}
        {% set hub_ns = namespace(hub_already_rehashed=false) %}
        {% set all_hub_columns = adapter.get_columns_in_relation(ref(hub.hub_name)) %}
        {% for column in all_hub_columns %}
            {% if column.name|lower == hub.hub_hashkey|lower + '_deprecated' %}
                {% set hub_ns.hub_already_rehashed = true %}
                {{ log('Hub ' ~ hub.hub_name ~ ' already rehashed, joining on _deprecated column', false) }}
            {% endif %}
        {% endfor %}

        {% if hub_ns.hub_already_rehashed %}
            {% set join_hashkey_col = hub.hub_hashkey + '_deprecated' %}
        {% else %}
            {% set join_hashkey_col = hub.hub_hashkey %}
        {% endif %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "new_hashkey_name": new_hub_hashkey_name,
            "hub_name": hub.hub_name,
            "hub_relation": ref(hub.hub_name),
            "hub_join_alias": hub_join_alias,
            "prefixed_business_keys": prefixed_business_keys,
            "join_hashkey_col": join_hashkey_col
        } %}

        {% do ns.hub_hashkeys.append(hub_hashkey_dict) %}
        {% do ns.hash_config_dict.update({new_hub_hashkey_name: prefixed_business_keys}) %}
        {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + prefixed_business_keys %}
    {% endfor %}

    {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + additional_hash_input_cols %}
    {% do ns.hash_config_dict.update({new_link_hashkey_name: ns.link_hashkey_input_cols}) %}

    {# Trino memory connector does not support UPDATE. Use CTAS + DROP + RENAME instead.
       Like the postgres implementation, JOIN with each hub table to obtain business keys
       (links store only hashkeys, not the underlying business keys). #}
    {% set existing_columns = adapter.get_columns_in_relation(link_relation) %}
    {% set clean_cols = [] %}
    {% for col in existing_columns %}
        {% if not col.name.lower().endswith('_new') and not col.name.lower().endswith('_deprecated') %}
            {% do clean_cols.append('outer_link.' ~ col.name) %}
        {% endif %}
    {% endfor %}

    {% set temp_identifier = link_relation.identifier ~ '_rehash_tmp' %}
    {% set temp_relation = api.Relation.create(
        database=link_relation.database,
        schema=link_relation.schema,
        identifier=temp_identifier
    ) %}

    {# Clean up any orphaned temp table from a previous failed run. #}
    {% set drop_tmp_sql %}DROP TABLE IF EXISTS {{ temp_relation }}{% endset %}
    {% do run_query(drop_tmp_sql) %}

    {# Step 1: CTAS — join link with each hub to get business keys, compute new hash values.
       hash_config_dict keys order: hub1_new, hub2_new, ..., link_new (must match UNION ALL column order). #}
    {{ log('Executing CTAS statement for link ' ~ link ~ '...', output_logs) }}
    {% set ctas_sql %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT
        {{ clean_cols | join(',\n        ') }},
        nh.{{ new_link_hashkey_name }}
        {% for hub_hashkey in ns.hub_hashkeys %}
        , nh.{{ hub_hashkey.new_hashkey_name }}
        {% endfor %}
    FROM {{ link_relation }} outer_link
    JOIN (

        SELECT
            link.{{ link_hashkey }},
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }}
        FROM {{ link_relation }} link
        {% for hub_hashkey in ns.hub_hashkeys %}
        LEFT JOIN {{ hub_hashkey.hub_relation }} {{ hub_hashkey.hub_join_alias }}
            ON link.{{ hub_hashkey.current_hashkey_name }} = {{ hub_hashkey.hub_join_alias }}.{{ hub_hashkey.join_hashkey_col }}
        {% endfor %}
        WHERE link.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT
            link.{{ link_hashkey }}
            {% for hub_hashkey in ns.hub_hashkeys %}
            , link.{{ hub_hashkey.current_hashkey_name }} AS {{ hub_hashkey.new_hashkey_name }}
            {% endfor %}
            , link.{{ link_hashkey }} AS {{ new_link_hashkey_name }}
        FROM {{ link_relation }} link
        WHERE link.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    ) nh
        ON nh.{{ link_hashkey }} = outer_link.{{ link_hashkey }}
    {% endset %}
    {% do run_query(ctas_sql) %}
    {{ log('CTAS completed!', output_logs) }}

    {# Step 2: Drop original table. #}
    {% set drop_sql %}DROP TABLE {{ link_relation }}{% endset %}
    {% do run_query(drop_sql) %}

    {# Step 3: Rename temp table to the original table name. #}
    {% set rename_table_sql %}ALTER TABLE {{ temp_relation }} RENAME TO {{ link_relation.identifier }}{% endset %}
    {% do run_query(rename_table_sql) %}
    {{ log('Link rehash (CTAS-based) completed for ' ~ link ~ '!', output_logs) }}

    {# Rename existing hash columns if overwrite is requested. #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {# Run each rename as a separate query — Trino does not support multi-statement execution. #}
        {% set rename1_sql = datavault4dbt.custom_get_rename_column_sql(relation=link_relation, old_col_name=link_hashkey, new_col_name=link_hashkey + '_deprecated') %}
        {% do run_query(rename1_sql) %}

        {% set rename2_sql = datavault4dbt.custom_get_rename_column_sql(relation=link_relation, old_col_name=new_link_hashkey_name, new_col_name=link_hashkey) %}
        {% do run_query(rename2_sql) %}

        {% for hub_hashkey in ns.hub_hashkeys %}
            {% set rename_hub1 = datavault4dbt.custom_get_rename_column_sql(relation=link_relation, old_col_name=hub_hashkey.current_hashkey_name, new_col_name=hub_hashkey.current_hashkey_name + '_deprecated') %}
            {% do run_query(rename_hub1) %}

            {% set rename_hub2 = datavault4dbt.custom_get_rename_column_sql(relation=link_relation, old_col_name=hub_hashkey.new_hashkey_name, new_col_name=hub_hashkey.current_hashkey_name) %}
            {% do run_query(rename_hub2) %}

            {% do ns.columns_to_drop.append({"name": hub_hashkey.current_hashkey_name + '_deprecated'}) %}
        {% endfor %}

        {% if drop_old_values %}
            {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=link_relation, remove_columns=ns.columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', true) }}
        {% endif %}

    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}


{% macro trino__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}
    {# Trino does not support UPDATE. The rehash logic uses CTAS in trino__rehash_single_link instead. #}
    {{ exceptions.raise_compiler_error("trino__link_update_statement is not supported. Use trino__rehash_single_link which uses CTAS-based rehashing.") }}
{% endmacro %}
