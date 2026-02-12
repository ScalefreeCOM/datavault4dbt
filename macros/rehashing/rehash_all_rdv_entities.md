{% docs rehash_all_rdv_entities %}

      This macro can rehash your entire RDV. It will process a single yml file/value and rehash every entity that's defined in it. 
      It will start with rehashing all hubs, then all links, then all satellites.

      How to use: 
          Since this is a macro, it could be called in a run-operation execution. But that will make it hard to pass a long yaml as a parameter. 
          Therefore, we recommend calling this macro in a new model, which allows to easily define a longer yaml. 

      
      {#
      Example: 

      new model 'rehash_model.sql'

      content: 

          {{ config(materialized='view') }}

          {% set entity_yaml %}
          config:
              overwrite_hash_values: true
              naming_conventions:
                  hashkey_syntax: hk_*
                  hub_hashkey_syntax: hk_*_h
                  link_hashkey_syntax: hk_*_l
                  hashdiff_syntax: hd_*
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
          ma_satellites:
              - name: customer_n0_ms
              hashkey: HK_CUSTOMER_H
              hashdiff: HD_CUSTOMER_N_MS
              ma_keys:
                  - O_ORDERKEY
              payload: 
                  - O_ORDERSTATUS
                  - O_ORDERPRIORITY
                  - O_CLERK
                  - O_SHIPPRIORITY
                  - O_COMMENT
                  - LEGACY_ORDERKEY
              parent_entity: customer_h
              business_keys:
                  - C_CUSTKEY
          nh_satellites: 
              - name: order_customer_n_ns
              hashkey: HK_ORDER_CUSTOMER_NL
              parent_entity: order_customer_nl
          hubs:
              - name: customer_h
              hashkey: hk_customer_h
              business_keys: 
                  - c_custkey
          links:
              - name: customer_nation_l
              link_hashkey: hk_customer_nation_l
              additional_hash_input_cols: []
              hub_config:
                  - hub_hashkey: hk_customer_h
                  hub_name: customer_h
                  business_keys:
                      - c_custkey
                  - hub_hashkey: hk_nation_h
                  hub_name: nation_h
                  business_keys:
                      - n_nationkey      
              - name: order_customer_nl
              link_hashkey: hk_order_customer_nl
              additional_hash_input_cols: []
              hub_config:
                  - hub_hashkey: hk_order_h
                  hub_name: order_h
                  business_keys: 
                      - o_orderkey
                  - hub_hashkey: hk_customer_h
                  hub_name: customer_h
                  business_keys:
                      - c_custkey
              - name: supplier_nation_l
              link_hashkey: hk_supplier_nation_l
              additional_hash_input_cols: []
              hub_config:
                  - hub_hashkey: hk_supplier_h
                  hub_name: supplier_h
                  business_keys: 
                      - s_suppkey
                  - hub_hashkey: hk_nation_h
                  hub_name: nation_h
                  business_keys:
                      - n_nationkey
              - name: part_supplier_l
              link_hashkey: hk_part_supplier_l
              additional_hash_input_cols: []
              hub_config:
                  - hub_hashkey: hk_supplier_h
                  hub_name: supplier_h
                  business_keys: 
                      - s_suppkey
                  - hub_hashkey: hk_part_h
                  hub_name: part_h
                  business_keys:
                      - p_partkey

          {% endset %}

          {{ datavault4dbt.rehash_all_rdv_entities(entity_yaml= entity_yaml, drop_old_values=true) }}

          SELECT 'test' as test

        #}

{% enddocs %}