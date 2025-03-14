{#
    Expects a yaml file that describes all hubs to be rehashed.

    Parameters: 
        hub_yaml: a yaml that describes all hubs to be rehashed
            Example: 
                config: 
                    overwrite_hash_values: true
                hubs:
                    - name: customer_h
                      hashkey: hk_customer_h
                      business_keys: 
                          - c_custkey

        drop_old_values: true|false (default true)
            If set to true, the old hashkeys will be automatically dropped. This will make your Hub structure look like before rehashing. 
            If set to false, the old hashkeys will remain in the hub, with a "_deprecated" suffix. 
    
#}

{% macro rehash_hubs(hub_yaml, drop_old_values=true) %}

    {% set ns = namespace(columns_to_drop=[]) %}

    {% set hub_dict = fromyaml(hub_yaml) %}

    {% set overwrite_hash_values = hub_dict.config.get('overwrite_hash_values', false) %}

    {% for hub in hub_dict.get('hubs') %}
        {% set specific_hub_overwrite_hash = hub.get('overwrite_hash_values', overwrite_hash_values) %}
         
        {% set columns_to_drop_list =  datavault4dbt.rehash_single_hub(hub=hub.name, 
                                                                    hashkey=hub.hashkey,
                                                                    business_keys=hub.business_keys,
                                                                    overwrite_hash_values=specific_hub_overwrite_hash,
                                                                    output_logs=false,
                                                                    drop_old_values=drop_old_values) %}
                        
        {{ log(hub.name ~ ' rehashed successfully.', true) }}     

        {% set columns_to_drop_dict = {'model_name': hub.name, 'columns_to_drop': columns_to_drop_list} %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}

    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}