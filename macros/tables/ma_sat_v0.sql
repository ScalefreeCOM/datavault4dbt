{%- macro ma_sat_v0(parent_hashkey, src_hashdiff, src_ma_key, src_payload, source_model, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('ma_sat_v0', 'datavault4dbt')(parent_hashkey=parent_hashkey,
                                         src_hashdiff=src_hashdiff,
                                         src_ma_key=src_ma_key,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model) }}

{%- endmacro -%}
