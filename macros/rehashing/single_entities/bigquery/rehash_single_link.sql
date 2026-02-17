{#

Works for standard links and non-historized links.

Example usage: 

dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'
--args '{link: link_01, link_hashkey: hashkey_link_single_source, overwrite_hash_values: true, hub_config: [{hub_hashkey: hashkey_single_source, hub_name: hub_01, business_keys: [o_orderkey]}]}'
#}



{% macro bigquery__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set ns = namespace(hub_hashkeys=[], new_hash_columns=[{"name": new_link_hashkey_name, "data_type": hash_datatype}], columns_to_drop=[{"name": link_hashkey + '_deprecated'}]) %}

    {% set link_relation = ref(link) %}

    {% set link_relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=(link)) %}

    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% set new_hub_hashkey_name =  hub.hub_hashkey ~ '_new' %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "new_hashkey_name": new_hub_hashkey_name,
            "hub_name": hub.hub_name,
            "hub_join_alias": hub_join_alias,
            "prefixed_business_keys": prefixed_business_keys
        } %}

        {% do ns.hub_hashkeys.append(hub_hashkey_dict) %}

        {% set new_hash_col_dict = {
            "name": new_hub_hashkey_name,
            "data_type": hash_datatype
        } %}

        {% do ns.new_hash_columns.append(new_hash_col_dict) %}

    {% endfor %}

    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql = datavault4dbt.link_update_statement(link_relation=link_relation,
                                                hub_hashkeys=ns.hub_hashkeys,
                                                link_hashkey=link_hashkey,
                                                new_link_hashkey_name=new_link_hashkey_name,
                                                additional_hash_input_cols=additional_hash_input_cols) %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...' ~ update_sql, output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ link ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}

    {# renaming existing hash columns #}
    {% if overwrite_hash_values %}
        {{ log('Replacing existing hash values with new ones...', output_logs) }}

        {% set overwrite_sql %}
        {# Rename Link Hashkey #}
            ALTER TABLE {{ link_relation }} 
                RENAME COLUMN {{ link_hashkey }} TO {{ link_hashkey }}_deprecated;

            {# Rename All Hub Hashkeys #}
            {% for hub_hashkey in ns.hub_hashkeys %}
                ALTER TABLE {{ link_relation }} 
                    RENAME COLUMN {{ hub_hashkey.current_hashkey_name }} TO {{ hub_hashkey.current_hashkey_name }}_deprecated;

                {# Prepare list of 'deprecated' hub hashkey columns to drop them later on. #}
                {% do ns.columns_to_drop.append({"name": hub_hashkey.current_hashkey_name + '_deprecated'}) %}

            {% endfor %}

        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        {% if drop_old_values %}
            {# Drop old table and rename _rehashed #}
            {% set old_table_name = link_relation %}
            {% set new_table_name = link_relation.database ~ '.' ~ link_relation.schema ~ '.' ~ link_relation.identifier ~ '_rehashed' %}

            {{ log('Dropping old table: ' ~ old_table_name, output_logs) }}
            {% do run_query(bigquery__drop_table(old_table_name)) %}

            {% set rename_sql = bigquery__get_rename_table_sql(new_table_name, link_relation.identifier) %}
            {{ log('Renaming rehashed table to original name: ' ~ rename_sql, output_logs) }}
            {% do run_query(rename_sql) %}

        {% endif %}

    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}


{% macro bigquery__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}

    {% set ns = namespace(link_hashkey_input_cols=[], hash_config_dict={}) %}
    
    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% for hub_hashkey in hub_hashkeys %}
        
        {% do ns.hash_config_dict.update({hub_hashkey.current_hashkey_name: hub_hashkey.prefixed_business_keys}) %}
        {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + hub_hashkey.prefixed_business_keys %}
    
    {% endfor %}

    {# Defining Hash config for new link hashkey. #}
    {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + additional_hash_input_cols %}
    {% do ns.hash_config_dict.update({link_hashkey: ns.link_hashkey_input_cols}) %}

    {{ log('hash_config: ' ~ ns.hash_config_dict, false)}}

    {% set update_sql %}
    CREATE TABLE {{ link_relation.database }}.{{ link_relation.schema  }}.{{link_relation.identifier ~ '_rehashed'}} AS (
        SELECT
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }},
            link.{{ ldts_alias }},
            link.{{ rsrc_alias }}
        FROM {{ link_relation }} link
        
        {% for hub in hub_hashkeys %}

            LEFT JOIN  {{ ref(hub.hub_name).render() }}  {{ hub.hub_join_alias }} 
                ON link.{{ hub.current_hashkey_name }} = {{ hub.hub_join_alias }}.{{ hub.current_hashkey_name }}

        {% endfor %}
        WHERE link.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
        
        UNION ALL
        SELECT * 
        FROM {{ link_relation }} link
            WHERE link.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    )
    {% endset %}


    {{ return(update_sql) }}

{% endmacro %}