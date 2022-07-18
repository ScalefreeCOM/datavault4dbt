{{ config(schema='public_release_test',
           materialized='view') }} 

{{ sat_v1(source_sat='opportunity_sfdc_data_lrn0_s',
          src_hk='hk_opportunity_h',
          src_hd='hd_opportunity_data_sfdc_lrn_s',
          src_ldts='ldts',
          ledts_alias='ledts') }}
