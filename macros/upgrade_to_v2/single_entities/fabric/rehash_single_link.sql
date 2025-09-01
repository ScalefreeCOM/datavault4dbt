{#

Works for standard links and non-historized links.

Example usage: 

dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'

#}



{% macro fabric__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% set old_link_hashkey_name = link_hashkey ~ '_deprecated' %}

    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'VARBINARY(8000)') %}

    {% set ns = namespace(hub_hashkeys=[], old_hash_columns=[{"name": old_link_hashkey_name, "data_type": hash_datatype}], new_hash_columns=[{"name": new_link_hashkey_name, "data_type": hash_datatype}], columns_to_drop=[{"name": old_link_hashkey_name}]) %}

    {% set link_relation = ref(link) %}

    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% set old_hub_hashkey_name =  hub.hub_hashkey ~ '_deprecated' %}
        {% set new_hub_hashkey_name =  hub.hub_hashkey ~ '_new' %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "old_hashkey_name": old_hub_hashkey_name,
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

        {% set old_hash_col_dict = {
            "name": old_hub_hashkey_name,
            "data_type": hash_datatype
        } %}

        {% do ns.old_hash_columns.append(old_hash_col_dict) %}

    {% endfor %}

    
    {# ALTER existing link to add deprecated link hashkey and deprecated hub hashkeys. #}
    {{ log('Executing ALTER TABLE statement...', output_logs) }}
    {{ datavault4dbt.alter_relation_add_remove_columns(relation=link_relation, add_columns=ns.old_hash_columns) }}
    {{ log('ALTER TABLE statement completed!', output_logs) }}

    {% set update_ns = namespace(depr_update_sql='') %}
    {# Update SQL statement to copy hashkey to _depr column  #}
    {% set update_ns.depr_update_sql %}
        UPDATE {{ link_relation }}
        SET 
            {{ old_link_hashkey_name }} = {{ link_hashkey }}
    {% endset %}

    {% for hub_hashkey in ns.hub_hashkeys %}

        {% set update_ns.depr_update_sql = update_ns.depr_update_sql + '\n,' + hub_hashkey.old_hashkey_name + '= ' + hub_hashkey.current_hashkey_name %}

    {% endfor %}

    {# Executing the UPDATE statement. #}
    {{ log('Executing UPDATE statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ update_ns.depr_update_sql ~ '*/' }}
    {% do run_query(update_ns.depr_update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}


    {% if overwrite_hash_values %}
        
        {% set new_link_hashkey_name = link_hashkey%}
        
        {% for hub_hashkey in ns.hub_hashkeys %}
            
            {% do ns.hub_hashkeys[loop.index0].update({'new_hashkey_name': ns.hub_hashkeys[loop.index0].current_hashkey_name}) %}
                
            {# Prepare list of 'deprecated' hub hashkey columns to drop them later on. #}
            {% do ns.columns_to_drop.append({"name": hub_hashkey.old_hashkey_name}) %}

        {% endfor %}

    {% else %}

        {# Alter existing Hub to add new hashkey column. #}
        {{ log('Executing ALTER TABLE statement...', output_logs) }}
        {{ datavault4dbt.alter_relation_add_remove_columns(relation=link_relation, add_columns=ns.new_hash_columns) }}
        {{ log('ALTER TABLE statement completed!', output_logs) }}
    {% endif %}

    {# generating the UPDATE statement that populates the new columns. #}
    {% set update_sql = datavault4dbt.link_update_statement(link_relation=link_relation,
                                                hub_hashkeys=ns.hub_hashkeys,
                                                link_hashkey=link_hashkey,
                                                new_link_hashkey_name=new_link_hashkey_name,
                                                additional_hash_input_cols=additional_hash_input_cols) %}

    {{ log('Executing UPDATE statement...', output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ hub ~ '\n' ~ update_sql ~ '*/' }}
    {% do run_query(update_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}


    {# Deleting old hashkey #}
    {% if drop_old_values or not overwrite_hash_values %}
        {{ datavault4dbt.alter_relation_add_remove_columns(relation=link_relation, remove_columns=ns.columns_to_drop) }}
        {{ log('Deprecated hashkey column removed!', output_logs) }}
    {% endif %}

    {{ return(columns_to_drop) }}

{% endmacro %}



{% macro fabric__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}

    {% set ns = namespace(link_hashkey_input_cols=[], hash_config_dict={}, update_sql_part1='', update_sql_part2='') %}

    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set old_link_hashkey_name = link_hashkey ~ '_deprecated' %}

    {% set ns.update_sql_part1 %}
    UPDATE {{ link_relation }}
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


    {% set ns.update_sql_part2 %}
    FROM (

        SELECT
            link.{{ old_link_hashkey_name }},
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }}
        FROM {{ link_relation }} link
    {% endset %}
        
        {% for hub in hub_hashkeys %}

            {% set hub_ns = namespace(hub_correct_hashkey=hub.current_hashkey_name) %}

            {#
                If hub entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
                hashkey column needs to be used for joining.

                Otherwise, the regular hashkey should be used for joining. 
            #}
            
            {% set all_hub_columns = adapter.get_columns_in_relation(ref(hub.hub_name)) %}
            {% for column in all_hub_columns %}
                {% if column.name|lower == hub.current_hashkey_name|lower + '_deprecated' %}
                    {% set hub_ns.hub_correct_hashkey = hub.old_hashkey_name %}
                    {{ log('Hub already hashed!', output_logs) }}
                {% endif %}
            {% endfor %}

            {% set ns.update_sql_part2 = ns.update_sql_part2 + '\n LEFT JOIN ' + ref(hub.hub_name).render() + ' ' + hub.hub_join_alias + '\n    ON link.' + hub.old_hashkey_name + ' = ' + hub.hub_join_alias + '.' + hub_ns.hub_correct_hashkey %}

        {% endfor %}

    {% set update_sql_part3 %}
        WHERE link.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL

        SELECT 
            link.{{ old_link_hashkey_name }}
            
            {% for hub in hub_hashkeys %}

            , link.{{ hub.current_hashkey_name }}

            {% endfor %}
            , link.{{ new_link_hashkey_name }}
            
        FROM {{ link_relation }} link
        WHERE link.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')
    ) nh
    WHERE {{ link_relation }}.{{ old_link_hashkey_name }} = nh.{{ old_link_hashkey_name }}
    {% endset %}

    {% set update_sql = ns.update_sql_part1 + ns.update_sql_part2 + update_sql_part3 %}

    {{ return(update_sql) }}

{% endmacro %}