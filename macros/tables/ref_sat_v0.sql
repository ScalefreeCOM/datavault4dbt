{#
Example model:

{{ config(materialized='incremental',
          schema='Core') }}

{%- set yaml_metadata -%}
source_model: stg_nation
parent_ref_keys: N_NATIONKEY
src_hashdiff: hd_nation_rs
src_payload:
    - N_COMMENT
    - N_NAME
    - N_REGIONKEY
{%- endset -%}      

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ datavault4dbt.ref_sat_v0(source_model=metadata_dict['source_model'],
                     parent_ref_keys=metadata_dict['parent_ref_keys'],
                     src_hashdiff=metadata_dict['src_hashdiff'],
                     src_payload=metadata_dict['src_payload']) }}

#}




{%- macro ref_sat_v0(parent_ref_keys, src_hashdiff, src_payload, source_model, src_ldts=none, src_rsrc=none, disable_hwm=false, source_is_single_batch=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('ref_sat_v0', 'datavault4dbt')(parent_ref_keys=parent_ref_keys,
                                         src_hashdiff=src_hashdiff,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model,
                                         disable_hwm=disable_hwm,
                                         source_is_single_batch=source_is_single_batch) 
    }}
{%- endmacro -%}