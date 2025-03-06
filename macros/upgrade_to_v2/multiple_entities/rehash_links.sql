

{% macro rehash_links(link_yaml) %}

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
                                                                        output_logs=false) %}
                        
        {{ log(link.name ~ ' rehashed successfully.', true) }}      

        {% set columns_to_drop_dict = {'model_name': link.name, 'columns_to_drop': columns_to_drop_list} %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}

    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}