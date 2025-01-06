{%- macro ref_hub(yaml_metadata=none, ref_keys=none, source_models=none, src_ldts=none, src_rsrc=none) -%}

    {% set ref_keys_description = "
    ref_keys::string|list of strings        Name of the reference key(s) available in the source model(s).
    " %}

    {% set source_models_description = "
    source_models::dictionary       Similar to other source_models parameters, e.g. in Hubs or Links. 
    " %}

    {% set src_ldts_description = "
    src_ldts::string                Name of the ldts column inside the source models. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set src_rsrc_description = "
    src_rsrc::string                Name of the rsrc column inside the source models. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {%- set ref_keys        = datavault4dbt.yaml_metadata_parser(name='ref_keys', yaml_metadata=yaml_metadata, parameter=ref_keys, required=True, documentation=ref_keys_description) -%}
    {%- set source_models   = datavault4dbt.yaml_metadata_parser(name='source_models', yaml_metadata=yaml_metadata, parameter=source_models, required=True, documentation=source_models_description) -%}
    {%- set src_ldts        = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc        = datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ return(adapter.dispatch('ref_hub', 'datavault4dbt')(ref_keys=ref_keys,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            source_models=source_models)) }}

{%- endmacro -%}