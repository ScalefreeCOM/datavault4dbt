{#
    This macro creates a non-historized satellite that should be materialized as an incremental table. It should be
    applied 'on top' of the staging layer, and is either connected to a Hub or a Link. Besides the missing hashdiff, a non-historized
    satellite applies the same loading logic as a regular version 0 satellite.
    Each satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.

    Features:
        - High-Perfomance loading of non-historized satellite data

    Parameters:

    parent_hashkey::string          Name of the hashkey column inside the stage of the object that this satellite is attached to.

                                    Examples:
                                        'hk_account_h'          The satellite would be attached to the hub account, which has the
                                                                column 'hk_account_h' as a hashkey column.

                                        'hk_account_contact_l'  The satellite would be attached to the link between account and contact,
                                                                which has the column 'hk_account_contact_l' as a hashkey column.

    src_payload::list of strings    A list of all the descriptive attributes that should be included in this satellite.

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

{%- macro nh_sat(parent_hashkey, src_payload, source_model, src_ldts=none, src_rsrc=none, source_is_single_batch=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('nh_sat', 'datavault4dbt')(parent_hashkey=parent_hashkey,
                                         src_payload=src_payload,
                                         src_ldts=src_ldts,
                                         src_rsrc=src_rsrc,
                                         source_model=source_model,
                                         source_is_single_batch=source_is_single_batch) }}

{%- endmacro -%}
