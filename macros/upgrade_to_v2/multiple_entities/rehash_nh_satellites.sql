

{% macro rehash_nh_satellites(nh_satellite_yaml, drop_old_values=true) %}

    {% set ns = namespace(columns_to_drop=[]) %}

    {% set nh_satellite_dict = fromyaml(nh_satellite_yaml) %}

    {% set overwrite_hash_values = nh_satellite_dict.config.get('overwrite_hash_values', false) %}

    {% for nh_satellite in nh_satellite_dict.get('nh_satellites') %}
        {% set specific_nh_satellite_overwrite_hash = nh_satellite.get('overwrite_hash_values', overwrite_hash_values) %}
         
        {% set columns_to_drop_list =  datavault4dbt.rehash_single_nh_satellite(nh_satellite=nh_satellite.name, 
                                                                                hashkey=nh_satellite.hashkey,
                                                                                parent_entity=nh_satellite.parent_entity,
                                                                                business_keys=nh_satellite.business_keys,
                                                                                overwrite_hash_values=specific_nh_satellite_overwrite_hash,
                                                                                output_logs=false,
                                                                                drop_old_values=drop_old_values) %}
                        
        {{ log(nh_satellite.name ~ ' rehashed successfully.', true) }}         

        {% set columns_to_drop_dict = {'model_name': nh_satellite.name, 'columns_to_drop': columns_to_drop_list} %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}
        
    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}