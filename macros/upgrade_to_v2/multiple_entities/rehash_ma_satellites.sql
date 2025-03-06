

{% macro rehash_ma_satellites(ma_satellite_yaml, drop_old_values=true) %}

    {% set ns = namespace(columns_to_drop=[]) %}

    {% set ma_satellite_dict = fromyaml(ma_satellite_yaml) %}

    {% set overwrite_hash_values = ma_satellite_dict.config.get('overwrite_hash_values', false) %}

    {% for ma_satellite in ma_satellite_dict.get('ma_satellites') %}
        {% set specific_ma_satellite_overwrite_hash = ma_satellite.get('overwrite_hash_values', overwrite_hash_values) %}
         
        {% set columns_to_drop_list =  datavault4dbt.rehash_single_ma_satellite(ma_satellite=ma_satellite.name, 
                                                                                hashkey=ma_satellite.hashkey,
                                                                                hashdiff=ma_satellite.hashdiff,
                                                                                ma_keys=ma_satellite.ma_keys,
                                                                                payload=ma_satellite.payload,
                                                                                parent_entity=ma_satellite.parent_entity,
                                                                                business_keys=ma_satellite.business_keys,
                                                                                overwrite_hash_values=specific_ma_satellite_overwrite_hash,
                                                                                output_logs=false,
                                                                                drop_old_values=drop_old_values) %}
                        
        {{ log(ma_satellite.name ~ ' rehashed successfully.', true) }}                
        
        {% set columns_to_drop_dict = {'model_name': ma_satellite.name, 'columns_to_drop': columns_to_drop_list} %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}

    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}