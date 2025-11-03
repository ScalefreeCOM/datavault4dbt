{#-
    This macro calculates a virtualized load end date on top of a version 0 satellite. This column is generated for
    usage in the PIT tables, and only virtualized to follow the insert-only approach. Usually one version 1 sat would
    be created for each version 0 sat. A version 1 satellite should be materialized as a view by default.
#}

{%- macro sat_v1(yaml_metadata=none, sat_v0=none, hashkey=none, hashdiff=none, src_ldts=none, src_rsrc=none, ledts_alias=none, add_is_current_flag=false, include_payload=true) -%}

    {% set sat_v0_description = "
    sat_v0::string                  Name of the underlying version 0 satellite.

                                    Examples:
                                        'account_data_sfdc_0_s'     This satellite would be the version 1 satellite of the underlying
                                                                    version 0 data satellite for account.
    " %}

    {% set hashkey_description = "
    hashkey::string                 Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a
                                    hub or a link. Needs to be similar to the 'parent_hashkey' parameter inside the sat_v0 model.

                                    Examples:
                                        'hk_account_h'          The satellite would be attached to the hub account, which has the
                                                                column 'hk_account_h' as a hashkey column.

                                        'hk_account_contact_l'  The satellite would be attached to the link between account and contact,
                                                                which has the column 'hk_account_contact_l' as a hashkey column.
    " %}

    {% set hashdiff_description = "
    hashdiff::string                Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the
                                    'src_hashdiff' pararmeter inside the sat_v0 model.

                                    Examples:
                                        'hd_account_data_sfdc_s'    Since we recommend naming the hashdiff column similar to the name
                                                                    of the satellite entity, just with a prefix, this would be the
                                                                    hashdiff column of the data satellite for account.
    " %}

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

    {% set include_payload_description = "
    include_payload::boolean        Optional parameter to specify if the v1 sat should have the payload columns from sat v0 or not. Default is true.
    " %}

    {%- set sat_v0              = datavault4dbt.yaml_metadata_parser(name='sat_v0', yaml_metadata=yaml_metadata, parameter=sat_v0, required=True, documentation=sat_v0_description) -%}
    {%- set hashkey             = datavault4dbt.yaml_metadata_parser(name='hashkey', yaml_metadata=yaml_metadata, parameter=hashkey, required=True, documentation=hashkey_description) -%}
    {%- set hashdiff            = datavault4dbt.yaml_metadata_parser(name='hashdiff', yaml_metadata=yaml_metadata, parameter=hashdiff, required=True, documentation=hashdiff_description) -%}
    {%- set src_ldts            = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set src_rsrc            = datavault4dbt.yaml_metadata_parser(name='src_rsrc', yaml_metadata=yaml_metadata, parameter=src_rsrc, required=False, documentation=src_rsrc_description) -%}
    {%- set ledts_alias         = datavault4dbt.yaml_metadata_parser(name='ledts_alias', yaml_metadata=yaml_metadata, parameter=ledts_alias, required=False, documentation=ledts_alias_description) -%}
    {%- set add_is_current_flag = datavault4dbt.yaml_metadata_parser(name='add_is_current_flag', yaml_metadata=yaml_metadata, parameter=add_is_current_flag, required=False, documentation=add_is_current_flag_description) -%}
    {%- set include_payload     = datavault4dbt.yaml_metadata_parser(name='include_payload', yaml_metadata=yaml_metadata, parameter=include_payload, required=False, documentation=include_payload_description) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}
    {%- set ledts_alias = datavault4dbt.replace_standard(ledts_alias, 'datavault4dbt.ledts_alias', 'ledts') -%}

    {%- if var('datavault4dbt.use_premium_package', False) == True -%}
        {{ datavault4dbt_premium_package.insert_metadata_sat_v1(sat_v0=sat_v0,
                                         hashkey=hashkey,
                                         hashdiff=hashdiff,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         ledts_alias=ledts_alias,
                                         add_is_current_flag=add_is_current_flag,
                                         include_payload=include_payload) }}
    {%- endif %}
    
    {{ adapter.dispatch('sat_v1', 'datavault4dbt')(sat_v0=sat_v0,
                                         hashkey=hashkey,
                                         hashdiff=hashdiff,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         ledts_alias=ledts_alias,
                                         add_is_current_flag=add_is_current_flag,
                                         include_payload=include_payload) }}

{%- endmacro -%}
