{{ config(materialized='view') }}
{{ config(schema='dbt_stage') }}

{%- set yaml_metadata -%}
source_table: "source_opportunity"
source_schema: "source_data"
hashed_columns: 
  hk_opportunity_h:
    - opportunity_key__c
  hk_contract_h:
    - contractnumber 
  hk_opportunity_contract_l:
    - opportunity_key__c
    - contractnumber
  hk_account_h:
    - account_key__c
  hk_opportunity_account_l:
    - opportunity_key__c
    - account_key__c
  hd_opportunity_data_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - planned_service_end_date__c
      - planned_service_start_date__c
      - consulting_support_commission__c
      - consulting_recruiter__c
      - training_services_failure_in_basic_tasks__c
      - data_quality_flaw__c
      - commission_paid__c
      - important_invoice_information__c
      - accounting_seed_project__c
      - vendor_tool_recipient__c
      - commission_exempted__c
      - lost_reason__c
      - stagename
      - hasoverduetask
      - internal_commission__c
      - lead_source_details__c
      - template_picklist__c
      - isprivate
      - commission_period__c
      - lost_reason_details__c
      - laststagechangedate
      - expectedrevenue
      - hasopenactivity
      - affiliate_commission_paid__c
      - iswon
      - affiliate_commission_amount__c
      - isclosed
      - closedate
      - totalopportunityquantity
  hd_opportunity_acctseed_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - acctseed__total_with_tax__c
      - acctseed__tax_amount__c
  hd_opportunity_data_sfdc_lrn_s:
    is_hashdiff: true
    columns:
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
  hd_opportunity_affiliate_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - ambassador__short_code__c
  hd_opportunity_ambassador_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - ambassador__referred_by_ambassador__c
      - ambassador__positive_opportunity_commission_amount__c
  hd_opportunity_quote_data_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - quote_expiry_date__c
      - syncedquoteid
  hd_opportunity_invoice_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - vat_rate_correction__c
      - calculated_commission__c
      - commission_date__c
      - invoicing_status__c
      - commision_approved__c
      - invoice_contact_assigned__c
      - basic_commission__c
  hd_opportunity_invoice_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - amount_incl_vat__c
      - billing_check__c
      - manual_vat__c
      - vat_rate_source__c
      - service_provisioning_country__c
      - vat_rate_suggestion__c
      - vat_invoice_area__c
      - service_tax_type__c
      - tax_text_code__c
      - account_billing_country__c
      - acctseed__trackingnumber__c
      - manual_date_of_service__c
      - vat_event_country__c
      - account_vat_number__c
  hd_opportunity_identifier_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - lastmodifiedbyid
      - gdrive_id__c
      - id
      - account_folder_name__c
      - ambassador__ambassador_referred_by__c
      - business_unit__c
      - campaignid
      - pricebook2id
      - cirrus_files_folder_name__c
      - createdbyid
      - accountid
      - ownerid
  hd_opportunity_juston_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - on_grossinvoice__c
      - on_createsingleinvoice__c
      - on_lasterror__c
      - on_emailinvoice__c
  hd_opportunity_leadfeeder_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - leadfeederapp__latestwebsitevisit__c
      - leadfeederapp__leadfeederlink__c
  hd_opportunity_identifier_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - lastamountchangedhistoryid
      - contractid
      - quote_key__c
      - lastclosedatechangedhistoryid
      - contactid
  hd_opportunity_activity_sfdc_hrn_s:
    is_hashdiff: true
    columns:
      - lastmodifieddate
      - lastvieweddate
      - lastreferenceddate
      - createddate
      - lastactivitydate
      - systemmodstamp
  hd_opportunity_lookup_helper_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - lh__lh_test_2__c
  hd_opportunity_invoice_legacy_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - on_printinvoice__c
  hd_opportunity_accounting_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - amount
      - probability
  hd_opportunity_juston_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - on_opportunitycurrency__c
  hd_opportunity_description_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - description
  hd_opportunity_data_sfdc_hrn_s:
    is_hashdiff: true
    columns:
      - nextstep
  hd_opportunity_linked_in_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - lid__linkedin_company_id__c
      - lid__is_influenced__c
  hd_opportunity_description_rich_sfdc_mrn_s:
    is_hashdiff: true
    columns:
      - description_rich__c
  hd_opportunity_accounting_sfdc_lrn_s:
    is_hashdiff: true
    columns:
      - fiscalyear
      - fiscalquarter
      - fiscal
      - currencyisocode
  hd_opportunity_juston_sfdc_mrp_s:
    is_hashdiff: true
    columns:
      - on_emailcc__c
rsrc: 'rsrc_file' 
ldts: 'edwLoadDate'
include_source_columns: true
prejoined_columns: 
  contractnumber:
    src_schema: "source_data"
    src_table: "source_contract"
    this_column_name: "ContractId"
    ref_column_name: "Id"
  account_key__c:
    src_schema: "source_data"
    src_table: "source_account"
    this_column_name: "AccountId"
    ref_column_name: "Id"

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