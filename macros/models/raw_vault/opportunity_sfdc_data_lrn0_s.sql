{{ config(schema='public_release_test',
           materialized='incremental',
           unique_key=['hk_opportunity_h', 'hd_opportunity_data_sfdc_lrn_s']) }} 



{%- set yaml_metadata -%}
source_model: "stage_opportunity" 
parent_hashkey: "hk_opportunity_h"
src_hashdiff: 'hd_opportunity_data_sfdc_lrn_s'
src_payload:
  - encryption_key__c
  - product_type__c
  - opportunity_name_uppercase__c
  - hasopportunitylineitem
  - leadsource
  - name
  - type
  - forecastcategory
  - forecastcategoryname
  - isdeleted
src_rsrc: "rsrc"
src_ldts: "ldts"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sat_v0(parent_hashkey=metadata_dict["parent_hashkey"],
                src_hashdiff=metadata_dict["src_hashdiff"],
                src_payload=metadata_dict["src_payload"],
                src_ldts=metadata_dict["src_ldts"],
                src_rsrc=metadata_dict["src_rsrc"],
                source_model=metadata_dict["source_model"])   }}