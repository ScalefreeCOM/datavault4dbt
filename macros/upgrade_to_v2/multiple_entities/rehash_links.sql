{#
    Expects a yaml file that describes all links to be rehashed.

    Parameters: 
        link_yaml: a yaml that describes all links to be rehashed
            Example: 
                config: 
                    overwrite_hash_values: true
                links:
                    - name: customer_nation_l
                      link_hashkey: hk_customer_nation_l
                      additional_hash_input_cols: []
                      hub_config:
                          - hub_hashkey: hk_customer_h
                          hub_name: customer_h
                          business_keys:
                              - c_custkey
                          - hub_hashkey: hk_nation_h
                          hub_name: nation_h
                          business_keys:
                              - n_nationkey      
                    - name: order_customer_nl
                      link_hashkey: hk_order_customer_nl
                      additional_hash_input_cols: []
                      hub_config:
                          - hub_hashkey: hk_order_h
                          hub_name: order_h
                          business_keys: 
                              - o_orderkey
                          - hub_hashkey: hk_customer_h
                          hub_name: customer_h
                          business_keys:
                              - c_custkey

        drop_old_values: true|false (default true)
            If set to true, the old hashkeys will be automatically dropped. This will make your Link structure look like before rehashing. 
            If set to false, the old hashkeys will remain in the link, with a "_deprecated" suffix. 
    
#}

{% macro rehash_links(link_yaml, drop_old_values=true) %}

    {% set ns = namespace(columns_to_drop=[]) %}

    {% set link_dict = fromyaml(link_yaml) %}

    {% set overwrite_hash_values = link_dict.config.get('overwrite_hash_values', false) %}

    {% for link in link_dict.get('links') %}
        {% set specific_link_overwrite_hash = link.get('overwrite_hash_values', overwrite_hash_values) %}
         
        {% set columns_to_drop_list =  datavault4dbt.rehash_single_link(link=link.name, 
                                                                        link_hashkey=link.link_hashkey,
                                                                        additional_hash_input_cols=link.additional_hash_input_cols,
                                                                        overwrite_hash_values=specific_link_overwrite_hash,
                                                                        hub_config=link.hub_config,
                                                                        output_logs=false,
                                                                        drop_old_values=drop_old_values) %}
                        
        {{ log(link.name ~ ' rehashed successfully.', true) }}      

        {% set columns_to_drop_dict = {'model_name': link.name, 'columns_to_drop': columns_to_drop_list} %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}

    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}