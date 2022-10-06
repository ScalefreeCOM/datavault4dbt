{{ config(materialized='incremental',
          unique_key='hk_creditcard_transactions_nl') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_creditcard_transactions_nl'
src_payload:
    - invoice_address
    - vendor_name
source_model: 'stage_creditcard_transactions'
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set parent_hashkey = metadata_dict['parent_hashkey'] -%}
{%- set src_payload = metadata_dict['src_payload'] -%}
{%- set source_model = metadata_dict['source_model'] -%}


{{ datavault4dbt.nh_link(link_hashkey=link_hashkey,
                         foreign_hashkeys=foreign_hashkeys,
                         payload=payload,
                         source_model=source_model) }}          
-------------------------------------------------------------
Description from macro file. Maybe talk about the use case here, which is:
Splitting data, that would usually go into a nhlink into a seperate satellite for privacy reasons.                         