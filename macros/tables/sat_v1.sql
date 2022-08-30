{#
    This macro calculates a virtualized load end date on top of a version 0 satellite. This column is generated for
    usage in the PIT tables, and only virtualized to follow the insert-only approach. Usually one version 1 sat would
    be created for each version 0 sat. A version 1 satellite should be materialized as a view by default.

    Parameters:

    sat_v0::string                  Name of the underlying version 0 satellite.

                                    Examples:
                                        'account_data_sfdc_0_s'     This satellite would be the version 1 satellite of the underlying
                                                                    version 0 data satellite for account.

    hashkey::string                 Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a
                                    hub or a link. Needs to be similar to the 'parent_hashkey' parameter inside the sat_v0 model.

                                    Examples:
                                        'hk_account_h'          The satellite would be attached to the hub account, which has the
                                                                column 'hk_account_h' as a hashkey column.

                                        'hk_account_contact_l'  The satellite would be attached to the link between account and contact,
                                                                which has the column 'hk_account_contact_l' as a hashkey column.

    hashdiff::string                Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the
                                    'src_hashdiff' pararmeter inside the sat_v0 model.

                                    Examples:
                                        'hd_account_data_sfdc_s'    Since we recommend naming the hashdiff column similar to the name
                                                                    of the satellite entity, just with a prefix, this would be the
                                                                    hashdiff column of the data satellite for account.

    src_ldts::string                Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.

    ledts_alias::string             Desired alias for the load end date column. Is optional, will use the global variable 'dbtvault_scalefree.ledts_alias' if
                                    set here.
    add_is_current_flag::boolean    Optional parameter to add a new column to the v1 sat based on the load end date timestamp (ledts). Default is false. If
                                    set to true it will add this is_current flag to the v1 sat. For each record this column will be set to true if the load
                                    end date time stamp is equal to the variable end of all times. If its not, then the record is not current therefore it
                                    will be set to false.
#}

{%- macro sat_v1(sat_v0, hashkey, hashdiff, src_ldts=none, src_rsrc=none, ledts_alias=none, add_is_current_flag=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}

    {%- set src_ldts = dbtvault_scalefree.replace_standard(src_ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.rsrc_alias', 'rsrc') -%}
    {%- set src_ledts = dbtvault_scalefree.replace_standard(src_ledts, 'dbtvault_scalefree.ledts_alias', 'ledts') -%}

    {{ adapter.dispatch('sat_v1', 'dbtvault_scalefree')(sat_v0=sat_v0,
                                         hashkey=hashkey,
                                         hashdiff=hashdiff,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         ledts_alias=ledts_alias,
                                         add_is_current_flag=add_is_current_flag) }}

{%- endmacro -%}
