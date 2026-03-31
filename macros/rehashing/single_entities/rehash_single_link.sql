{#
    Usage example:
    dbt run-operation rehash_single_link --args '{link: customer_nation_l, link_hashkey: hk_customer_nation_l, overwrite_hash_values: true, hub_config: [{hub_hashkey: hk_customer_h, hub_name: customer_h, business_keys: [c_custkey]}, {hub_hashkey: hk_nation_h, hub_name: nation_h, business_keys: [n_nationkey]}]}'
#}

{% macro rehash_single_link(link, link_hashkey, hub_config, additional_hash_input_cols=[], overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {{ adapter.dispatch('rehash_single_link', 'datavault4dbt')(link=link,
                                                                link_hashkey=link_hashkey,
                                                                hub_config = hub_config,
                                                                additional_hash_input_cols = additional_hash_input_cols,
                                                                overwrite_hash_values=overwrite_hash_values,
                                                                output_logs=output_logs,
                                                                drop_old_values=drop_old_values) }}
    
{% endmacro %}

{% macro link_update_statement(link_relation, hub_hashkeys, link_hashkey, new_link_hashkey_name, additional_hash_input_cols=[]) %}

    {{ adapter.dispatch('link_update_statement', 'datavault4dbt')(link_relation=link_relation,
                                                                hub_hashkeys=hub_hashkeys,
                                                                link_hashkey=link_hashkey,
                                                                new_link_hashkey_name=new_link_hashkey_name,
                                                                additional_hash_input_cols=additional_hash_input_cols) }}

{% endmacro %}
