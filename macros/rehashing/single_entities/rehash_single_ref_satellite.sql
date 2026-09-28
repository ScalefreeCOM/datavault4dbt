{#
    Usage example:
    dbt run-operation rehash_single_ref_satellite --args '{ref_satellite: customer_ref_rs, refkey: RK_CUSTOMER_R, hashdiff: HD_CUSTOMER_RS, payload: [C_NAME, C_REGION], overwrite_hash_values: true}'
#}

{% macro rehash_single_ref_satellite(ref_satellite, refkey, hashdiff, payload, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {{ adapter.dispatch('rehash_single_ref_satellite', 'datavault4dbt')(ref_satellite=ref_satellite,
                                                                        refkey=refkey,
                                                                        hashdiff=hashdiff,
                                                                        payload=payload,
                                                                        overwrite_hash_values=overwrite_hash_values,
                                                                        output_logs=output_logs,
                                                                        drop_old_values=drop_old_values)}}

{% endmacro %}


{% macro ref_satellite_update_statement(satellite_relation, new_hashdiff_name, refkey, hashdiff, ldts_col, hash_config_dict) %}

    {{ adapter.dispatch('ref_satellite_update_statement', 'datavault4dbt')(satellite_relation=satellite_relation,
                                                                       new_hashdiff_name=new_hashdiff_name,
                                                                       refkey=refkey,
                                                                       hashdiff=hashdiff,
                                                                       ldts_col=ldts_col,
                                                                       hash_config_dict=hash_config_dict) }}

{% endmacro %}


