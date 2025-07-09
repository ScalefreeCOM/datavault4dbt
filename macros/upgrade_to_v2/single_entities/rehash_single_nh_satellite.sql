{#

    Works for Non-Historized Satellites either attached to Hubs or (NH)Links. 
    If attached to Hub: 
        Define Business Keys of Hub 
        OR Rehash Hub first, without overwriting hash values.

    If attached to (NH)Link:
        Rehash (NH)Link first, without overwriting hash values.

    Usage example:
    dbt run-operation rehash_single_nh_satellite --args '{nh_satellite: order_customer_n_ns, hashkey: HK_ORDER_CUSTOMER_NL, parent_entity: order_customer_nl, overwrite_hash_values: true}'
#}

{% macro rehash_single_nh_satellite(nh_satellite, hashkey, parent_entity, business_keys=none, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {{ adapter.dispatch('rehash_single_ma_satellite', 'datavault4dbt')(nh_satellite=nh_satellite,
                                                                        hashkey=hashkey,
                                                                        parent_entity=parent_entity,
                                                                        business_keys=business_keys,
                                                                        overwrite_hash_values=overwrite_hash_values,
                                                                        output_logs=output_logs,
                                                                        drop_old_values=drop_old_values)}}

{% endmacro %}


{% macro nh_satellite_update_statement(nh_satellite_relation, new_hashkey_name, hashkey, ldts_col, parent_relation, hash_config_dict=none) %}

    {{ adapter.dispatch('nh_satellite_update_statement', 'datavault4dbt')(nh_satellite_relation=nh_satellite_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       hashkey=hashkey, 
                                                                       ldts_col=ldts_col,
                                                                       hash_config_dict=hash_config_dict,
                                                                       parent_relation=parent_relation) }}

{% endmacro %}
