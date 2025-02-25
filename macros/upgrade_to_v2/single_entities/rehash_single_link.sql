{#
hub_config:
    - hub_hashkey: hk_customer_h
      hub_name: customer_h
      business_keys:
        - c_custkey
    - hub_hashkey: hk_nation_h
      hub_name: nation_h
      business_keys:
        - n_nationkey

#}



{% macro rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols, overwrite_hash_values=false, output_logs=true) %}

    {% set new_link_hashkey_name = link_hashkey ~ '_new' %}
    {% set hash_datatype = var('datavault4dbt.hash_datatype', 'STRING') %}
    {% set ns = namespace(hub_hashkeys=[], new_hash_columns=[{"name": new_link_hashkey_name, "data_type": hash_datatype}]) %}

    {% set link_relation = ref(link) %}

    {% for hub in hub_config %}
        {% set hub_join_alias = 'hub' ~ loop.index %}
        {% set prefixed_business_keys = datavault4dbt.prefix(columns=hub.business_keys, prefix_str=hub_join_alias).split(',') %}
        {% set new_hub_hashkey_name =  hub.hub_hashkey ~ '_new' %}

        {% set hub_hashkey_dict = {
            "current_hashkey_name": hub.hub_hashkey,
            "new_hashkey_name": new_hub_hashkey_name,
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

    



{% endmacro %}