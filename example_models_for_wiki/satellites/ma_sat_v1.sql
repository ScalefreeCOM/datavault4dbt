{{ config(materialized='view') }}

{%- set yaml_metadata -%}
sat_v0: 'contact_phonenumer_v0_mas'
hashkey: 'hk_contact_h'
hashdiff: 'hd_contact_phonenumber_mas' 
ma_attribute:
    - phone_type
    - iid
ledts_alias: 'valid_to'
add_is_current_flag: true
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set sat_v0 = metadata_dict['sat_v0'] -%}
{%- set hashkey = metadata_dict['hashkey'] -%}
{%- set hashdiff = metadata_dict['hashdiff'] -%}
{%- set ledts_alias = metadata_dict['ledts_alias'] -%}
{%- set ma_attribute = metadata_dict['ma_attribute'] -%}
{%- set add_is_current_flag = metadata_dict['add_is_current_flag'] -%}

{{ datavault4dbt.sat_v1(sat_v0=sat_v0,
                        hashkey=hashkey,
                        hashdiff=hashdiff,
                        ma_attribute=ma_attribute,
                        ledts_alias=ledts_alias,
                        add_is_current_flag=add_is_current_flag) }}
-------------------------------------------------------------------------------
Description from macro file. Please point out, that for the underlying sat v0, the regular sat_v0 macro is used.                         