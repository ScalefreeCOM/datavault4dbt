{#

    Parameters:
        ref_satellite_yaml: a yaml that describes all reference satellites to be rehashed
            Example:
                config:
                    overwrite_hash_values: true
                ref_satellites:
                    - name: customer_ref_rs
                      refkey: RK_CUSTOMER_R
                      hashdiff: HD_CUSTOMER_RS
                      payload:
                          - C_NAME
                          - C_REGION
                    - name: part_ref_rs
                      refkey: RK_PART_R
                      hashdiff: HD_PART_RS
                      payload:
                          - P_NAME
                          - P_TYPE
                          - P_SIZE

        drop_old_values: true|false (default true)
            If set to true, the old hash values will be automatically dropped. This will make your ref satellite structure look like before rehashing.
            If set to false, the old hash values will remain in the ref satellite, with a "_deprecated" suffix.

#}

{% macro rehash_ref_satellites(ref_satellite_yaml, drop_old_values=true) %}
    {% set ns = namespace(columns_to_drop=[]) %}

    {% set ref_satellite_dict = fromyaml(ref_satellite_yaml) %}

    {% if ref_satellite_dict.get('ref_satellites') is not none %}

        {% set overwrite_hash_values = ref_satellite_dict.config.get('overwrite_hash_values', false) %}

        {% for ref_satellite in ref_satellite_dict.get('ref_satellites') %}
            {% set specific_overwrite_hash = ref_satellite.get('overwrite_hash_values', overwrite_hash_values) %}

            {% if execute %}
                {% set columns_to_drop_list = datavault4dbt.rehash_single_ref_satellite(ref_satellite=ref_satellite.name,
                                                                                        refkey=ref_satellite.refkey,
                                                                                        hashdiff=ref_satellite.hashdiff,
                                                                                        payload=ref_satellite.payload,
                                                                                        overwrite_hash_values=specific_overwrite_hash,
                                                                                        output_logs=false,
                                                                                        drop_old_values=drop_old_values) %}

                {{ log(ref_satellite.name ~ ' rehashed successfully.', true) }}

                {% set columns_to_drop_dict = {'model_name': ref_satellite.name, 'columns_to_drop': (columns_to_drop_list | trim) } %}

                {% do ns.columns_to_drop.append(columns_to_drop_dict) %}
            {% endif %}

        {% endfor %}
    {% endif %}

    {{ return(ns.columns_to_drop) }}

{% endmacro %}
