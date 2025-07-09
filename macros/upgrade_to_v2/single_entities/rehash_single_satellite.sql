{#
    Usage example:
    dbt run-operation rehash_single_satellite --args '{satellite: customer_n0_s, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_S, payload: [C_ACCTBAL, C_MKTSEGMENT, C_COMMENT], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro rehash_single_satellite(satellite, hashkey, hashdiff, payload, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {{ adapter.dispatch('rehash_single_ma_satellite', 'datavault4dbt')(satellite=satellite,
                                                                        hashkey=hashkey,
                                                                        hashdiff=hashdiff,
                                                                        payload=payload,
                                                                        parent_entity=parent_entity,
                                                                        business_keys=business_keys,
                                                                        overwrite_hash_values=overwrite_hash_values,
                                                                        output_logs=output_logs,
                                                                        drop_old_values=drop_old_values)}}

{% endmacro %}


{% macro satellite_update_statement(satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, ldts_col, hash_config_dict, parent_relation) %}

    {{ adapter.dispatch('satellite_update_statement', 'datavault4dbt')(satellite_relation=satellite_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       new_hashdiff_name=new_hashdiff_name,
                                                                       hashkey=hashkey, 
                                                                       ldts_col=ldts_col,
                                                                       hash_config_dict=hash_config_dict,
                                                                       parent_relation=parent_relation) }}

{% endmacro %}
