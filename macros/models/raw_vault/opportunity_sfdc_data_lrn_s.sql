{{ config(schema='public_release_test',
           materialized='view') }} 

{%- set yaml_metadata -%}
source_sat: 'opportunity_sfdc_data_lrn0_s'
src_hk: "hk_opportunity_h"
src_hd: 'hd_opportunity_data_sfdc_lrn_s'
src_ldts: "ldts"
ledts_alias='ledts'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sat_v1(source_sat=metadata_dict['source_sat'],
          src_hk=metadata_dict['src_hk'],
          src_hd=metadata_dict['src_hd'],
          src_ldts=metadata_dict['src_ldts'],
          ledts_alias=metadata_dict['ledts_alias']) }}
