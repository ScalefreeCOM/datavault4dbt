{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_account_h'
src_hashdiff: 'hd_account_s'
src_payload:
    - name
    - address
    - phone
    - email    
source_model: 'stage_account'
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set parent_hashkey = metadata_dict['parent_hashkey'] -%}
{%- set src_hashdiff = metadata_dict['src_hashdiff'] -%}
{%- set source_model = metadata_dict['source_model'] -%}


{{ datavault4dbt.sat_v0(parent_hashkey=parent_hashkey,
                        src_hashdiff=src_hashdiff,
                        source_model=source_model) }}
-----------------------------------------------------------------------------
For description see general macro file. Regular Hub Satellite in Version 0. Payload needs to be the same, as what is defined
in the staging area as input for the corresponding hashdiff column.                        