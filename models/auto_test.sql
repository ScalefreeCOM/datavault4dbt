{{ config(materialized='view') }}
{{ config(schema='dbt_stage') }}

{%- set yaml_metadata -%}
source_table: source_solution
source_schema: source_data
hashed_columns: 
  hk_solution_h:
    - solutionnumber
  hd_solution_activity_sfdc_hrn_s:
    is_hashdiff: true
    columns:
      - createddate
      - lastmodifieddate
      - lastmodifiedbyid
      - systemmodstamp
      - lastvieweddate
      - lastreferenceddate
  hd_solution_data_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - isdeleted
      - solutionname
      - ispublished
      - ispublishedinpublickb
      - status
      - isreviewed
      - timesused
      - currencyisocode
      - ishtml
      - topic__c
      - mantis_id__c
      - survey_url__c
      - attach_io_url__c
      - case_number__c
      - mantis_url__c
      - scalefree_url__c
  hd_solution_data_sfdc_lsp_s:
    is_hashdiff: true
    columns:
      - encryption_key__c
  hd_solution_description_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - solutionnote
      - introduction__c
      - problem_description__c
      - background_information__c
      - solution__c
      - related_information__c
  hd_solution_identifier_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - id
      - ownerid
      - createdbyid
  hd_solution_twitter_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - sf4twitter__twitterid__c
      - sf4twitter__twitter_summary__c

rsrc: 'rsrc_file' 
ldts: 'edwLoadDate'
include_source_columns: true
prejoined_columns:
  contact_key__c:
    src_schema: "source_data"
    src_table: "source_contact"
    this_column_name: "sf4twitter__Contact__c"
    ref_column_name: "id"


{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ stage(include_source_columns=metadata_dict['include_source_columns'],
                  source_table=metadata_dict['source_table'],
                  source_schema=metadata_dict['source_schema'],
                  hashed_columns=metadata_dict['hashed_columns'],
                  ranked_columns=none,
                  rsrc=metadata_dict['rsrc'],
                  ldts=metadata_dict['ldts'],
                  prejoined_columns=metadata_dict['prejoined_columns']) }}
