{#

Works for standard links and non-historized links.

Example usage: 

dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'

#}



{% macro snowflake__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set ns = namespace(hub_hashkeys=[], new_hash_columns=[{"name": new_link_hashkey_name, "data_type": hash_datatype}], columns_to_drop=[{"name": link_hashkey + '_deprecated'}]) %}

    {% set link_relation = ref(link) %}

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

    {# ALTER existing link to add new link hashkey and new hub hashkeys. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ alter_relation_add_remove_columns(relation=link_relation, add_columns=ns.new_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

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
            {{ datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=link_hashkey, new_col_name=link_hashkey + '_deprecated') }}
            {{ datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=new_link_hashkey_name, new_col_name=link_hashkey) }}
            
            {# Rename All Hub Hashkeys #}
            {% for hub_hashkey in ns.hub_hashkeys %}
                {{ datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=hub_hashkey.current_hashkey_name, new_col_name=hub_hashkey.current_hashkey_name + '_deprecated') }}
                {{ datavault4dbt.get_rename_column_sql(relation=link_relation, old_col_name=hub_hashkey.new_hashkey_name, new_col_name=hub_hashkey.current_hashkey_name) }}

                {# Prepare list of 'deprecated' hub hashkey columns to drop them later on. #}
                {% do ns.columns_to_drop.append({"name": hub_hashkey.current_hashkey_name + '_deprecated'}) %}

            {% endfor %}

        {% endset %}

        {% do run_query(overwrite_sql) %}
        
        {% if drop_old_values %}
            {{ alter_relation_add_remove_columns(relation=link_relation, remove_columns=ns.columns_to_drop) }}
            {{ log('Existing Hash values overwritten!', output_logs) }}
        {% endif %}

    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}


{% macro snowflake__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}

    {% set ns = namespace(link_hashkey_input_cols=[], hash_config_dict={}, update_sql_part1='', update_sql_part2='') %}

    {% set ns.update_sql_part1 %}
    UPDATE {{ link_relation }} link
    SET 
        {{ new_link_hashkey_name }} = nh.{{ new_link_hashkey_name }}

    {% endset %}

        {% for hub_hashkey in hub_hashkeys %}
            
            {% set ns.update_sql_part1 = ns.update_sql_part1 + '\n,' + hub_hashkey.new_hashkey_name + ' = nh.' + hub_hashkey.new_hashkey_name %}
            
            {% do ns.hash_config_dict.update({hub_hashkey.new_hashkey_name: hub_hashkey.prefixed_business_keys}) %}
            {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + hub_hashkey.prefixed_business_keys %}
        
        {% endfor %}

        {# Defining Hash config for new link hashkey. #}
        {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + additional_hash_input_cols %}
        {% do ns.hash_config_dict.update({new_link_hashkey_name: ns.link_hashkey_input_cols}) %}

    {{ log('hash_config: ' ~ ns.hash_config_dict, false)}}

    {% set ns.update_sql_part2 %}
    FROM (

        SELECT
            link.{{ link_hashkey }},
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }}
        FROM {{ link_relation }} link
    {% endset %}
        
        {% for hub in hub_hashkeys %}

            {% set ns.update_sql_part2 = ns.update_sql_part2 + '\n LEFT JOIN ' + ref(hub.hub_name).render() + ' ' + hub.hub_join_alias + '\n    ON link.' + hub.current_hashkey_name + ' = ' + hub.hub_join_alias + '.' + hub.current_hashkey_name %}

        {% endfor %}

    {% set update_sql_part3 %}
    ) nh
    WHERE link.{{ link_hashkey }} = nh.{{ link_hashkey }}
    {% endset %}

    {% set update_sql = ns.update_sql_part1 + ns.update_sql_part2 + update_sql_part3 %}

    {{ return(update_sql) }}

{% endmacro %}