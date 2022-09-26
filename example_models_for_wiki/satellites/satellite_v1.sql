{{ config(materialized='view') }}

{%- set yaml_metadata -%}
sat_v0: 'account_v0_s'
hashkey: 'hk_account_h'
hashdiff: 'hd_account_s'   
ledts_alias: 'loadenddate'
add_is_current_flag: true
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set sat_v0 = metadata_dict['sat_v0'] -%}
{%- set hashkey = metadata_dict['hashkey'] -%}
{%- set hashdiff = metadata_dict['hashdiff'] -%}
{%- set ledts_alias = metadata_dict['ledts_alias'] -%}
{%- set add_is_current_flag = metadata_dict['add_is_current_flag'] -%}

{{ datavault4dbt.sat_v1(sat_v0=sat_v0,
                        hashkey=hashkey,
                        hashdiff=hashdiff,
                        ledts_alias=ledts_alias,
                        add_is_current_flag=add_is_current_flag) }}
-----------------------------------------------------------------------------
For description see general macro file. Regular Hub Satellite in Version 1. Definitions here need to match the ones in version 0 satellite.
Say explicitly why ledts_alias and add_is_current_flag are set here. for ledts alias the default value out of the global variable would
be used if not set here. add_is_current_flag would be set to false by default.       