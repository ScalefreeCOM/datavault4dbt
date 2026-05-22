{%- macro ref_sat_v1(yaml_metadata=none, ref_sat_v0=none, ref_keys=none, hashdiff=none, src_ldts=none, src_rsrc=none, ledts_alias=none, add_is_current_flag=false) -%}

    {% set ref_sat_v0_description = "
    ref_sat_v0::string              Name of the underlying ref_sat_v0 dbt model
    " %}

    {% set ref_keys_description = "
    ref_keys::string | list of strings          Name(s) of the reference key(s) in the underlying reference sat v0.
    " %}

    {% set hashdiff_description = "
    hashdiff::string                Name of the Hashdiff column in the underlying reference sat v0.
    "%}

    {% set src_ldts_description = "
    src_ldts::string                Name of the ldts column inside the source models. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set src_rsrc_description = "
    src_rsrc::string                Name of the rsrc column inside the source models. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.
    " %}    

    {% set ledts_alias_description = "
    ledts_alias::string             Desired alias for the load end date column. Is optional, will use the global variable 'datavault4dbt.ledts_alias' if
                                    set here.
    " %}

    {% set add_is_current_flag_description = "
    add_is_current_flag::boolean    Optional parameter to add a new column to the v1 sat based on the load end date timestamp (ledts). Default is false. If
                                    set to true it will add this is_current flag to the v1 sat. For each record this column will be set to true if the load
                                    end date time stamp is equal to the variable end of all times. If its not, then the record is not current therefore it
                                    will be set to false.
    " %}

    {%- set ref_sat_v0          = datavault4dbt.yaml_metadata_parser(name='ref_sat_v0', yaml_metadata=yaml_metadata, parameter=ref_sat_v0, required=True, documentation=ref_sat_v0_description) -%}
    {%- set ref_keys            = datavault4dbt.yaml_metadata_parser(name='ref_keys', yaml_metadata=yaml_metadata, parameter=ref_keys, required=True, documentation=ref_keys_description) -%}
    {%- set hashdiff            = datavault4dbt.yaml_metadata_parser(name='hashdiff', yaml_metadata=yaml_metadata, parameter=hashdiff, required=True, documentation=hashdiff_description) -%}
    {%- set src_ldts            = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc            = datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}
    {%- set ledts_alias         = datavault4dbt.yaml_metadata_parser(name='ledts_alias', yaml_metadata=yaml_metadata, parameter=ledts_alias, required=False, documentation=ledts_alias_description) -%}
    {%- set add_is_current_flag = datavault4dbt.yaml_metadata_parser(name='add_is_current_flag', yaml_metadata=yaml_metadata, parameter=add_is_current_flag, required=False, documentation=add_is_current_flag_description) -%}
    
    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set ledts_alias = datavault4dbt.replace_standard(ledts_alias, 'datavault4dbt.ledts_alias', 'ledts') -%}

    {# For Fusion static_analysis overwrite #}
    {% set static_analysis_config = datavault4dbt.get_static_analysis_config('ref_sat_v1') %}
    {%- if datavault4dbt.is_something(static_analysis_config) -%}
        {{ config(static_analysis='off') }}
    {%- endif -%}

    {%- if var('datavault4dbt.use_premium_package', False) == True -%}
        {{ datavault4dbt_premium_package.insert_metadata_ref_sat_v1(ref_sat_v0=ref_sat_v0,
                                         ref_keys=ref_keys,
                                         hashdiff=hashdiff,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         ledts_alias=ledts_alias,
                                         add_is_current_flag=add_is_current_flag) }}
    {%- endif %}
    
    {{ adapter.dispatch('ref_sat_v1', 'datavault4dbt')(ref_sat_v0=ref_sat_v0,
                                         ref_keys=ref_keys,
                                         hashdiff=hashdiff,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         ledts_alias=ledts_alias,
                                         add_is_current_flag=add_is_current_flag) }}

{%- endmacro -%}