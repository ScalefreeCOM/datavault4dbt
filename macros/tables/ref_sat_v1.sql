{#
Example model:

{{ config(materialized='view',
          schema='Core') }}

{%- set yaml_metadata -%}
ref_sat_v0: nation_rs
ref_keys: N_NATIONKEY
hashdiff: hd_nation_rs
add_is_current_flag: true
{%- endset -%}      

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ datavault4dbt.ref_sat_v1(ref_sat_v0=metadata_dict['ref_sat_v0'],
                     ref_keys=metadata_dict['ref_keys'],
                     hashdiff=metadata_dict['hashdiff'],
                     add_is_current_flag=metadata_dict['add_is_current_flag']) }}

#}



{%- macro ref_sat_v1(ref_sat_v0, ref_keys, hashdiff, src_ldts=none, src_rsrc=none, ledts_alias=none, add_is_current_flag=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set src_ledts = datavault4dbt.replace_standard(src_ledts, 'datavault4dbt.ledts_alias', 'ledts') -%}

    {{ adapter.dispatch('ref_sat_v1', 'datavault4dbt')(ref_sat_v0=ref_sat_v0,
                                         ref_keys=ref_keys,
                                         hashdiff=hashdiff,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         ledts_alias=ledts_alias,
                                         add_is_current_flag=add_is_current_flag) }}

{%- endmacro -%}