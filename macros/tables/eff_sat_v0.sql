{%- macro eff_sat_v0(source_model, tracked_hashkey,  src_ldts=none, src_rsrc=none, is_active_alias=none, source_is_single_batch=true, disable_hwm=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set is_active_alias = datavault4dbt.replace_standard(is_active_alias, 'datavault4dbt.is_active_alias', 'deleted_flag') -%}

    {{ return(adapter.dispatch('eff_sat_v0', 'datavault4dbt')(tracked_hashkey=tracked_hashkey,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         is_active_alias=is_active_alias,
                                         source_model=source_model,
                                         source_is_single_batch=source_is_single_batch,
                                         disable_hwm=disable_hwm) )
    }}
{%- endmacro -%}