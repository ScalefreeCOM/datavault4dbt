{#

Works for standard links and non-historized links.

Example usage: 

dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'
--args '{link: link_01, link_hashkey: hashkey_link_single_source, overwrite_hash_values: true, hub_config: [{hub_hashkey: hashkey_single_source, hub_name: hub_01, business_keys: [o_orderkey]}]}'
#}



{% macro bigquery__rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {% if overwrite_hash_values %}
        {% set new_link_hashkey_name = link_hashkey %}
    {% else %}
        {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% endif %}

    {% set ns = namespace(hub_hashkeys=[], columns_to_drop=[{"name": link_hashkey + '_deprecated'}]) %}

    {% set link_relation = ref(link) %}

    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% if overwrite_hash_values %}
            {% set new_hub_hashkey_name =  hub.hub_hashkey %}
        {% else %}
            {% set new_hub_hashkey_name =  hub.hub_hashkey ~ '_new' %}
        {% endif %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "new_hashkey_name": new_hub_hashkey_name,
            "old_hashkey_name": hub.hub_hashkey + '_deprecated',
            "hub_name": hub.hub_name,
            "hub_join_alias": hub_join_alias,
            "prefixed_business_keys": prefixed_business_keys,
            "join_hashkey_col": hub.hub_hashkey
        } %}

        {% do ns.hub_hashkeys.append(hub_hashkey_dict) %}

        {% set column_to_drop = {"name": hub.hub_hashkey + '_deprecated'} %}

        {% do ns.columns_to_drop.append(column_to_drop) %}

    {% endfor %}

    {% set rename_sql = get_rename_table_sql(link_relation, link_relation.identifier ~ '_deprecated') %}
    {% do run_query(rename_sql) %}

    {# generating the CREATE statement that populates the new columns. #}
    {% set create_sql = datavault4dbt.link_update_statement(link_relation=link_relation,
                                                hub_hashkeys=ns.hub_hashkeys,
                                                link_hashkey=link_hashkey,
                                                new_link_hashkey_name=new_link_hashkey_name,
                                                additional_hash_input_cols=additional_hash_input_cols) %}

    {# Executing the CREATE statement. #}
    {{ log('Executing CREATE statement...' ~ create_sql, output_logs) }}
    {{ '/* UPDATE STATEMENT FOR ' ~ link ~ '\n' ~ create_sql ~ '*/' }}
    {% do run_query(create_sql) %}
    {{ log('UPDATE statement completed!', output_logs) }}


    {% if drop_old_values %}
        {# Drop old table and rename _rehashed #}
        {% set old_table_relation = make_temp_relation(link_relation,suffix='_deprecated') %}

        {{ log('Dropping old table: ' ~ old_table_name, output_logs) }}
        {% do run_query(drop_table(old_table_name)) %}
        {{ datavault4dbt.custom_alter_relation_add_remove_columns(relation=link_relation, remove_columns=ns.columns_to_drop) }}

    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}


{% macro bigquery__link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols) %}

    {% set ns = namespace(link_hashkey_input_cols=[], hash_config_dict={}, hubs=hub_hashkeys) %}
    
    {% set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') %}
    {% set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') %}

    {% set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') %}
    {% set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') %}

    {% set old_link_hashkey_name = link_hashkey + '_deprecated'%}
    {% set old_table_relation = make_temp_relation(link_relation,suffix='_deprecated') %}



    {% for hub_hashkey in hub_hashkeys %}
        
        {% do ns.hash_config_dict.update({hub_hashkey.new_hashkey_name: hub_hashkey.prefixed_business_keys}) %}
        {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + hub_hashkey.prefixed_business_keys %}
    
    {% endfor %}

    {# Defining Hash config for new link hashkey. #}
    {% set ns.link_hashkey_input_cols = ns.link_hashkey_input_cols + additional_hash_input_cols %}
    {% do ns.hash_config_dict.update({new_link_hashkey_name: ns.link_hashkey_input_cols}) %}

    {{ log('hash_config: ' ~ ns.hash_config_dict, false)}}


    {% set update_sql %}
    CREATE TABLE {{ link_relation }} AS (
        SELECT
            link.{{ link_hashkey }} as {{ old_link_hashkey_name }},
            {%- for hub in ns.hubs -%}
                {%- set hub_ns = namespace(hub_already_rehashed=false) -%}

                {#
                    If hub entity is rehashed already (via rehash_all_rdv_entities macro), the "_deprecated"
                    hashkey column needs to be used for joining.

                    Otherwise, the regular hashkey should be used for joining. 
                #}
                {%- set all_hub_columns = adapter.get_columns_in_relation(ref(hub.hub_name)) -%}
                {%- for column in all_hub_columns -%}
                    {%- if column.name|lower == hub.current_hashkey_name|lower + '_deprecated' -%}
                        {%- set hub_ns.hub_already_rehashed = true -%}
                        {{ log('Hub already hashed!', false) }}
                    {%- endif -%}
                {%- endfor -%}

                {%- if hub_ns.hub_already_rehashed -%}
                    {# {% set hub.join_hashkey_col.update() = hub.old_hashkey_name %} #}
                    {%- do hub.update({'join_hashkey_col': hub.old_hashkey_name}) -%}
                {%- endif %}
                {{ hub.hub_join_alias }}.{{ hub.join_hashkey_col }} as {{ hub.old_hashkey_name }},
            {% endfor %}
            
            {{ datavault4dbt.hash_columns(columns=ns.hash_config_dict) }},

            link.{{ ldts_alias }},
            link.{{ rsrc_alias }}
        FROM {{ old_table_relation }} link
        
        {%- for hub in ns.hubs %}

            LEFT JOIN  {{ ref(hub.hub_name).render() }}  {{ hub.hub_join_alias }} 
                ON link.{{ hub.current_hashkey_name }} = {{ hub.hub_join_alias }}.{{ hub.join_hashkey_col }}

        {%- endfor %}
        WHERE link.{{ rsrc_alias }} NOT IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

        UNION ALL
        SELECT 
            link.{{ link_hashkey }} as {{ old_link_hashkey_name }},
            {% for hub in ns.hubs %}
                {{ hub.current_hashkey_name }} as {{ hub.old_hashkey_name }},
            {% endfor %}
            link.{{ link_hashkey }} as {{ new_link_hashkey_name }},
            {% for hub in ns.hubs %}
                {{ hub.current_hashkey_name }} as {{ hub.new_hashkey_name }},
            {% endfor %}
            link.{{ ldts_alias }},
            link.{{ rsrc_alias }}
        FROM {{ old_table_relation }} link
        WHERE link.{{ rsrc_alias }} IN ('{{ unknown_value_rsrc }}', '{{ error_value_rsrc }}')

    )
    {% endset %}


    {{ return(update_sql) }}

{% endmacro %}