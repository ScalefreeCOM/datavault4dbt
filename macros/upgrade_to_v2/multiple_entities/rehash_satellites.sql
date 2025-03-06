

{% macro rehash_satellites(satellite_yaml, drop_old_values=true) %}

    {% set ns = namespace(columns_to_drop=[]) %}

    {% set satellite_dict = fromyaml(satellite_yaml) %}

    {% set overwrite_hash_values = satellite_dict.config.get('overwrite_hash_values', false) %}

    {% for satellite in satellite_dict.get('satellites') %}
        {% set specific_satellite_overwrite_hash = satellite.get('overwrite_hash_values', overwrite_hash_values) %}
         
        {% set columns_to_drop_list =  datavault4dbt.rehash_single_satellite(satellite=satellite.name, 
                                                                                hashkey=satellite.hashkey,
                                                                                hashdiff=satellite.hashdiff,
                                                                                payload=satellite.payload,
                                                                                parent_entity=satellite.parent_entity,
                                                                                business_keys=satellite.business_keys,
                                                                                overwrite_hash_values=specific_satellite_overwrite_hash,
                                                                                output_logs=false,
                                                                                drop_old_values=drop_old_values) %}
                        
        {{ log(satellite.name ~ ' rehashed successfully.', true) }}             

        {% set columns_to_drop_dict = {'model_name': satellite.name, 'columns_to_drop': columns_to_drop_list} %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}
   
    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}