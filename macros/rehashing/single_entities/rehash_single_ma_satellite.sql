{#
    Usage example:
    dbt run-operation rehash_single_ma_satellite --args '{ma_satellite: customer_n0_ms, hashkey: HK_CUSTOMER_H, hashdiff: HD_CUSTOMER_N_MS, ma_keys: [O_ORDERKEY], payload: [O_ORDERSTATUS, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, LEGACY_ORDERKEY], parent_entity: customer_h, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro rehash_single_ma_satellite(ma_satellite, hashkey, hashdiff, ma_keys, payload, parent_entity, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {{ adapter.dispatch('rehash_single_ma_satellite', 'datavault4dbt')(ma_satellite=ma_satellite,
                                                                        hashkey=hashkey,
                                                                        hashdiff=hashdiff,
                                                                        ma_keys=ma_keys,
                                                                        payload=payload,
                                                                        parent_entity=parent_entity,
                                                                        business_keys=business_keys,
                                                                        overwrite_hash_values=overwrite_hash_values,
                                                                        output_logs=output_logs,
                                                                        drop_old_values=drop_old_values)}}

{% endmacro %}


{% macro ma_satellite_update_statement(ma_satellite_relation, new_hashkey_name, new_hashdiff_name, hashkey, business_key_list, ma_keys, ldts_col, hash_config_dict, parent_relation) %}

    {{ adapter.dispatch('ma_satellite_update_statement', 'datavault4dbt')(ma_satellite_relation=ma_satellite_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       new_hashdiff_name=new_hashdiff_name,
                                                                       hashkey=hashkey, 
                                                                       business_key_list=business_key_list,
                                                                       ma_keys=ma_keys,
                                                                       ldts_col=ldts_col,
                                                                       hash_config_dict=hash_config_dict,
                                                                       parent_relation=parent_relation) }}

{% endmacro %}
