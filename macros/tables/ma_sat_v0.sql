{#
    This macro creates a multi-active satellite version 0, meaning that it should be materialized as an incremental table. It should be
    applied 'on top' of the staging layer, and is either connected to a Hub or a Link. On top of each version 0 multi-active satellite, a version
    1 multi-active satellite should be created, using the ma_sat_v1 macro. This extends the v0 satellite by a virtually calculated load end date.
    Each satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.
    If a stage model is defined as multi-active, all satellites out of that stage model need to be implemented as multi-active satellites.

    Features:
        - Can handle multiple updates per batch, without losing intermediate changes. therefore initial loading is supported.
        - Using a dynamic high-water-mark to optimize loading performance of multiple loads
#}

{%- macro ma_sat_v0(yaml_metadata=none, parent_hashkey=none, src_hashdiff=none, src_ma_key=none, src_payload=none, source_model=none, src_ldts=none, src_rsrc=none, additional_columns=none) -%}

    {% set parent_hashkey_description = "
    parent_hashkey::string                  Name of the hashkey column inside the stage of the object that this satellite is attached to.

                                            Examples:
                                                'hk_account_h'          The satellite would be attached to the hub account, which has the
                                                                        column 'hk_account_h' as a hashkey column.

                                                'hk_account_contact_l'  The satellite would be attached to the link between account and contact,
                                                                        which has the column 'hk_account_contact_l' as a hashkey column.
    " %}

    {% set src_hashdiff_description = "
    src_hashdiff::string                    Name of the hashdiff column of this satellite, that was created inside the staging area and is
                                            calculated out of the entire payload of this satellite. The stage must hold one hashdiff per
                                            satellite entity.

                                            Examples:
                                                'hd_account_data_sfdc_s'    Since we recommend naming the hashdiff column similar to the name
                                                                            of the satellite entity, just with a prefix, this would be the
                                                                            hashdiff column of the data satellite for account.
    " %}

    {% set src_ma_key_description = "
    src_ma_key::string|list of strings      Name(s) of the multi-active keys inside the staging area. Need to be the same ones, as
                                            defined in the stage model.

                                            Examples:
                                                'phonetype'                 The column 'phonetype' is the multi-active key inside the source
                                                                            model. That means, there is always only one combination of hashkey
                                                                            and multi-active key at a time.

                                                ['phonetype', 'company']    In this case, the combination of the two columns 'phonetype' and 'company'
                                                                            is treated as the multi-active key.
    " %}

    {% set src_payload_description = "
    src_payload::list of strings            A list of all the descriptive attributes that should be included in this satellite. Needs to be the
                                            columns that are fed into the hashdiff calculation of this satellite. Do not include the multi-active
                                            key in the payload of a multi-active satellite, it is included automatically!

                                            Examples:
                                                ['name', 'address', 'country', 'phone', 'email']    This satellite would hold the columns 'name',
                                                                                                    'address', 'country', 'phone' and 'email', coming
                                                                                                    out of the underlying staging area.
    " %}

    {% set source_model_description = "
    source_model::string                    Name of the underlying staging model, must be available inside dbt as a model.

                                            Examples:
                                                'stage_account'     This satellite is loaded out of the stage for account.
    " %}

    {% set src_ldts_description = "
    src_ldts::string                        Name of the ldts column inside the source model. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                            Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set src_rsrc_description = "
    src_rsrc::string                        Name of the rsrc column inside the source model. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                            Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set additional_columns_description = "
    additional_columns::list | string       Additional columns from source system or derived columns which should be part of the ma_sat.
                                            The columns need to be available in all source models.
                                            Optional parameter, defaults to empty list.
    " %}

    {%- set parent_hashkey      = datavault4dbt.yaml_metadata_parser(name='parent_hashkey', yaml_metadata=yaml_metadata, parameter=parent_hashkey, required=True, documentation=parent_hashkey_description) -%}
    {%- set src_hashdiff        = datavault4dbt.yaml_metadata_parser(name='src_hashdiff', yaml_metadata=yaml_metadata, parameter=src_hashdiff, required=True, documentation=src_hashdiff_description) -%}
    {%- set src_ma_key          = datavault4dbt.yaml_metadata_parser(name='src_ma_key', yaml_metadata=yaml_metadata, parameter=src_ma_key, required=True, documentation=src_ma_key_description) -%}
    {%- set src_payload         = datavault4dbt.yaml_metadata_parser(name='src_payload', yaml_metadata=yaml_metadata, parameter=src_payload, required=True, documentation=src_payload_description) -%}
    {%- set source_model        = datavault4dbt.yaml_metadata_parser(name='source_model', yaml_metadata=yaml_metadata, parameter=source_model, required=True, documentation=source_model_description) -%}
    {%- set src_ldts            = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc            = datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}
    {%- set additional_columns  = datavault4dbt.yaml_metadata_parser(name='additional_columns', yaml_metadata=yaml_metadata, parameter=additional_columns, required=False, documentation=additional_columns_description) -%}
    
    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {# For Fusion static_analysis overwrite #}
    {% set static_analysis_config = datavault4dbt.get_static_analysis_config('ma_sat_v0') %}
    {%- if datavault4dbt.is_something(static_analysis_config) -%}
        {{ config(static_analysis='off') }}
    {%- endif -%}

    {%- if var('datavault4dbt.use_premium_package', False) == True -%}
        {{ datavault4dbt_premium_package.insert_metadata_ma_sat_v0(parent_hashkey=parent_hashkey,
                                         src_hashdiff=src_hashdiff,
                                         src_ma_key=src_ma_key,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model) }}
    {%- endif %}

    {{ adapter.dispatch('ma_sat_v0', 'datavault4dbt')(parent_hashkey=parent_hashkey,
                                         src_hashdiff=src_hashdiff,
                                         src_ma_key=src_ma_key,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model,
                                         additional_columns=additional_columns) }}

{%- endmacro -%}
