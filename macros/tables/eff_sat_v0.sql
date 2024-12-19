{%- macro eff_sat_v0(yaml_metadata=none, source_model=none, tracked_hashkey=none,  src_ldts=none, src_rsrc=none, is_active_alias=none, source_is_single_batch=true, disable_hwm=false) -%}

    {% set source_model =           datavault4dbt.yaml_metadata_parser(name='source_model', yaml_metadata=yaml_metadata, parameter=source_model, required=True, documentation='Name of the source model') %}
    {% set tracked_hashkey =        datavault4dbt.yaml_metadata_parser(name='tracked_hashkey', yaml_metadata=yaml_metadata, parameter=tracked_hashkey, required=True, documentation='Name of the hashkey column to be tracked') %}
    {% set src_ldts =               datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation='Name of the loaddate column in the source model. Optional.') %}
    {% set src_rsrc =               datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation='Name of the record source column in the source model. Optional.') %}
    {% set is_active_alias =        datavault4dbt.yaml_metadata_parser(name='is_active_alias', yaml_metadata=yaml_metadata, parameter=is_active_alias, required=False, documentation='Name of the new active flag column. Optional.') %}
    {% set source_is_single_batch = datavault4dbt.yaml_metadata_parser(name='source_is_single_batch', yaml_metadata=yaml_metadata, parameter=source_is_single_batch, required=False, documentation='Whether the source contains only one batch. Optional, default True.') %}
    {% set disable_hwm =            datavault4dbt.yaml_metadata_parser(name='disable_hwm', yaml_metadata=yaml_metadata, parameter=disable_hwm, required=False, documentation='Whether the High Water Mark should be disabled or not. Optional.') %}
    
    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set is_active_alias = datavault4dbt.replace_standard(is_active_alias, 'datavault4dbt.is_active_alias', 'is_active') -%}

    {{ return(adapter.dispatch('eff_sat_v0', 'datavault4dbt')(tracked_hashkey=tracked_hashkey,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         is_active_alias=is_active_alias,
                                         source_model=source_model,
                                         source_is_single_batch=source_is_single_batch,
                                         disable_hwm=disable_hwm) )
    }}
    
{%- endmacro -%}