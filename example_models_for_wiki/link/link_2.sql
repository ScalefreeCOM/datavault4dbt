{{ config(materialized='incremental',
          unique_key='hk_opportunity_account_l') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_opportunity_account_l'
foreign_hashkeys: 
    - 'hk_opportunity_h'
    - 'hk_account_h'
source_models:
    stage_opportunity:
        rsrc_static: '*/SALESFORCE/Opportunity/*'
    stage_account:
        rsrc_static: '*/SAP/Account/*'
        link_hk: 'hashkey_account_opportunity'
        fk_columns: 
            - hashkey_opportunity
            - hashkey_account
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set link_hashkey = metadata_dict['link_hashkey'] -%}
{%- set foreign_hashkeys = metadata_dict['foreign_hashkeys'] -%}
{%- set source_models = metadata_dict['source_models'] -%}


{{ datavault4dbt.link(link_hashkey=link_hashkey,
        foreign_hashkeys=foreign_hashkeys,
        source_models=source_models) }}

---------------------------------------------------------------------------
For Description please transfer everything from the original macro file. Multi Source Link, with the second link having different column names.
Therefor "link_hk" and "fk_columns" is defined for that source. -> Source mapping
