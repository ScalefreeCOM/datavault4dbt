{%- macro ref_hub(yaml_metadata=none, ref_keys=none, source_models=none, src_ldts=none, src_rsrc=none, additional_columns=none) -%}

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

    {% set additional_columns_description = "
    additional_columns::list | string       Additional columns from source system or derived columns which should be part of the ref_hub.
                                            The columns need to be available in all source models.
                                            Optional parameter, defaults to empty list.
    " %}

    {%- set ref_keys            = datavault4dbt.yaml_metadata_parser(name='ref_keys', yaml_metadata=yaml_metadata, parameter=ref_keys, required=True, documentation=ref_keys_description) -%}
    {%- set source_models       = datavault4dbt.yaml_metadata_parser(name='source_models', yaml_metadata=yaml_metadata, parameter=source_models, required=True, documentation=source_models_description) -%}
    {%- set src_ldts            = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc            = datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}
    {%- set additional_columns  = datavault4dbt.yaml_metadata_parser(name='additional_columns', yaml_metadata=yaml_metadata, parameter=additional_columns, required=False, documentation=additional_columns_description) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {# For Fusion static_analysis overwrite #}
    {% set static_analysis_config = datavault4dbt.get_static_analysis_config('ref_hub') %}
    {%- if datavault4dbt.is_something(static_analysis_config) -%}
        {{ config(static_analysis='off') }}
    {%- endif -%}

    {%- if var('datavault4dbt.use_premium_package', False) == True -%}
        {{ datavault4dbt_premium_package.insert_metadata_ref_hub(ref_keys=ref_keys,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            source_models=source_models) }}
    {%- endif %}
    
    {{ return(adapter.dispatch('ref_hub', 'datavault4dbt')(ref_keys=ref_keys,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            source_models=source_models,
                                                            additional_columns=additional_columns)) }}

{%- endmacro -%}