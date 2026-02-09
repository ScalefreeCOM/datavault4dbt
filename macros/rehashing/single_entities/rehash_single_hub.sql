{#
    Usage example:
    dbt run-operation rehash_single_hub --args '{hub: customer_h, hashkey: HK_CUSTOMER_H, business_keys: C_CUSTKEY, overwrite_hash_values: true}'
#}

{% macro rehash_single_hub(hub, hashkey, business_keys, overwrite_hash_values=false, output_logs=true, drop_old_values=true) %}

    {{ adapter.dispatch('rehash_single_hub', 'datavault4dbt')(hub=hub,
                                                                hashkey=hashkey,
                                                                business_keys=business_keys, 
                                                                overwrite_hash_values=overwrite_hash_values,
                                                                output_logs=output_logs,
                                                                drop_old_values=drop_old_values) }}
    
{% endmacro %}

{% macro hub_update_statement(hub_relation, new_hashkey_name, hashkey, hash_config_dict) %}

    {{ adapter.dispatch('hub_update_statement', 'datavault4dbt')(hub_relation=hub_relation,
                                                                       new_hashkey_name=new_hashkey_name,
                                                                       hashkey=hashkey, 
                                                                       hash_config_dict=hash_config_dict) }}

{% endmacro %}
