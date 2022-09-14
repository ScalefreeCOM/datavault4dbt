{{ config(materialized='view') }}
{{ config(schema='dbt_stage') }}

{%- set yaml_metadata -%}
source_model: 
  "source_data": "source_account"
hashed_columns: 
  hk_account_h:
    - Account_Key__c
rsrc: 'rsrc_file'
ldts: 'edwLoadDate'
include_source_columns: true
derived_columns:
  type_lalala: 
    value: '!STAGE'
    datatype: 'STRING'


{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ datavault4dbt.stage(include_source_columns=metadata_dict['include_source_columns'],
                  source_model=metadata_dict['source_model'],
                  hashed_columns=metadata_dict['hashed_columns'],
                  prejoined_columns=none,
                  ranked_columns=none,
                  derived_columns=metadata_dict['derived_columns'],
                  missing_columns=none,
                  rsrc=metadata_dict['rsrc'],
                  ldts=metadata_dict['ldts']) }}