{#-
    This macro creates a standard satellite version 0, meaning that it should be materialized as an incremental table. It should be
    applied 'on top' of the staging layer, and is either connected to a Hub or a Link. On top of each version 0 satellite, a version
    1 satellite should be created, using the sat_v1 macro. This extends the v0 satellite by a virtually calculated load end date.
    Each satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.

    Features:
        - Can handle multiple updates per batch, without losing intermediate changes. therefore initial loading is supported.
        - Using a dynamic high-water-mark to optimize loading performance of multiple loads

    Parameters:

    parent_hashkey::string          Name of the hashkey column inside the stage of the object that this satellite is attached to.

                                    Examples:
                                        'hk_account_h'          The satellite would be attached to the hub account, which has the
                                                                column 'hk_account_h' as a hashkey column.

                                        'hk_account_contact_l'  The satellite would be attached to the link between account and contact,
                                                                which has the column 'hk_account_contact_l' as a hashkey column.

    src_hashdiff::string            Name of the hashdiff column of this satellite, that was created inside the staging area and is
                                    calculated out of the entire payload of this satellite. The stage must hold one hashdiff per
                                    satellite entity.

                                    Examples:
                                        'hd_account_data_sfdc_s'    Since we recommend naming the hashdiff column similar to the name
                                                                    of the satellite entity, just with a prefix, this would be the
                                                                    hashdiff column of the data satellite for account.

    src_payload::list of strings    A list of all the descriptive attributes that should be included in this satellite. Needs to be the
                                    columns that are fed into the hashdiff calculation of this satellite.

                                    Examples:
                                        ['name', 'address', 'country', 'phone', 'email']    This satellite would hold the columns 'name',
                                                                                            'address', 'country', 'phone' and 'email', coming
                                                                                            out of the underlying staging area.

    source_model::string            Name of the underlying staging model, must be available inside dbt as a model.

                                    Examples:
                                        'stage_account'     This satellite is loaded out of the stage for account.

    src_ldts::string                Name of the ldts column inside the source model. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                Name of the rsrc column inside the source model. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.

#}

{%- macro sat_v0(parent_hashkey, src_hashdiff, src_payload, source_model, src_ldts=none, src_rsrc=none, disable_hwm=false, source_is_single_batch=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts, src_rsrc, and ledts_alias are not set. #}
    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('sat_v0', 'datavault4dbt')(parent_hashkey=parent_hashkey,
                                         src_hashdiff=src_hashdiff,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model,
                                         disable_hwm=disable_hwm,
                                         source_is_single_batch=source_is_single_batch) 
    }}
{%- endmacro -%}
