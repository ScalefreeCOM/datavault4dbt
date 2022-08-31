{{ config(schema='public_release_test',
           materialized='view') }} 

{{ sat_v1(sat_v0='opportunity_sfdc_data_lrn0_s',
          hashkey='hk_opportunity_h',
          hashdiff='hd_opportunity_data_sfdc_lrn_s',
          src_ldts='ldts',
          ledts_alias='ledts') }}
