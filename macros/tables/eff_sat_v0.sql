{%- macro eff_sat_v0(source_models, tracked_hashkey,  src_ldts=none, src_rsrc=none, deleted_flag_alias=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set deleted_flag_alias = datavault4dbt.replace_standard(deleted_flag_alias, 'datavault4dbt.deleted_flag_alias', 'deleted_flag') -%}

    {{ return(adapter.dispatch('eff_sat_v0', 'datavault4dbt')(tracked_hashkey=tracked_hashkey,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         deleted_flag_alias=deleted_flag_alias,
                                         source_models=source_models) )
    }}
{%- endmacro -%}