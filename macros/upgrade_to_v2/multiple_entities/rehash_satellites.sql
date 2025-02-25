

{% macro rehash_satellites(satellite_yaml) %}

    {% set satellite_dict = fromyaml(satellite_yaml) %}

    {% set overwrite_hash_values = satellite_dict.config.get('overwrite_hash_values', false) %}

    {% for satellite in satellite_dict.get('satellites') %}
        {% set specific_satellite_overwrite_hash = satellite.get('overwrite_hash_values', overwrite_hash_values) %}
         
        {% do datavault4dbt.rehash_single_satellite(satellite=satellite.name, 
                                       hashkey=satellite.hashkey,
                                       hashdiff=satellite.hashdiff,
                                       payload=satellite.payload,
                                       parent_entity=satellite.parent_entity,
                                       business_keys=satellite.business_keys,
                                       overwrite_hash_values=specific_satellite_overwrite_hash,
                                       output_logs=false) %}
                        
        {{ log(satellite.name ~ ' rehashed successfully.', true) }}                
    {% endfor %}

{% endmacro %}