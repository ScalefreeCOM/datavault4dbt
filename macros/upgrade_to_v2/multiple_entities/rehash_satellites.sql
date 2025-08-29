{#

    Parameters: 
        satellite_yaml: a yaml that describes all standard satellites to be rehashed
            Example: 
                config: 
                    overwrite_hash_values: true
                satellites:
                    - name: customer_n0_s
                      hashkey: HK_CUSTOMER_H
                      hashdiff: HD_CUSTOMER_N_S
                      payload: 
                          - C_ACCTBAL
                          - C_MKTSEGMENT
                          - C_COMMENT
                      parent_entity: customer_h
                      business_keys:
                          - C_CUSTKEY
                    - name: customer_p0_s
                      hashkey: hk_customer_h
                      hashdiff: hd_customer_p_s
                      payload: 
                          - c_name
                          - c_address
                          - c_phone
                      parent_entity: customer_h
                      business_keys:
                          - c_custkey
                    - name: part_supplier_n0_s
                      hashkey: hk_part_supplier_l
                      hashdiff: hd_part_supplier_n_s
                      payload: 
                          - ps_availqty
                          - ps_supplycost
                          - ps_comment
                      parent_entity: part_supplier_l

        drop_old_values: true|false (default true)
            If set to true, the old hash values will be automatically dropped. This will make your satellite structure look like before rehashing. 
            If set to false, the old hash values will remain in the satellite, with a "_deprecated" suffix. 
    
#}

{% macro rehash_satellites(satellite_yaml, drop_old_values=true) %}
    {% set ns = namespace(columns_to_drop=[]) %}

    {% set satellite_dict = fromyaml(satellite_yaml) %}

    {% set overwrite_hash_values = satellite_dict.config.get('overwrite_hash_values', false) %}

    {% for satellite in satellite_dict.get('satellites') %}
        {% set specific_satellite_overwrite_hash = satellite.get('overwrite_hash_values', overwrite_hash_values) %}
{{log(drop_old_values,true)}}

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

        {% set columns_to_drop_dict = {'model_name': satellite.name, 'columns_to_drop': (columns_to_drop_list | trim) } %}

        {% do ns.columns_to_drop.append(columns_to_drop_dict) %}
   
    {% endfor %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}