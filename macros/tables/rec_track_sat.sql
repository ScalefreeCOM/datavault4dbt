{#
    This macro creates a Record Tracking Satellite and is most commonly used to track the appearances of hashkeys (calculated out of business keys)
    inside one or multiple source systems. This can either be the hashkey of a hub, or the hashkey of a link. Therefor Record Tracking Satellites can
    be build both for Hubs and Links. Typically if a hub is loaded from three sources, the corresponding Record Tracking Satellite would track
    the same three sources, since they apparently share the same business definition. For each source a rsrc_static must be defined, and optionally
    the name of the hashkey column inside that source, if it deviates between sources.

    Parameters:

    tracked_hashkey::string         The name of the hashkey column you want to track. Needs to be available in the underlying staging layer. If you want to track multiple
                                    hashkeys out of one stage, you need to create one record tracking satellite for each hashkey. 

                                    Examples:
                                        "hk_contact_h"              This record tracking satellite tracks the appearance of the hashkey for the contact hub.

                                        "hk_contact_account_l"      This record tracking satellite tracks the appearance of the hashkey for the link between contacts and accounts.

    source_models::dictionary       Dictionary with information about the source model. The key of the dict is the name of the source model, and the value  
                                    is another dictionary. This inner dictionary requires to have the keys 'rsrc_static', and optionally the key 'hk_column'.

                                    Examples: 
                                        {'stage_account': {'hk_column': 'hk_account_h',                         This record tracking satellite tracks the hashkey "hk_account_h" inside the
                                                        'rsrc_static': '*/SAP/Accounts/*'}}                     source model named "stage_account".

                                        {'stage_contact': {'rsrc_static': '*/SALESFORCE/Contact/*'},            This tracks the appearance of one hub hashkey that is loaded from the two source
                                        'stage_partner': {'hk_column': 'hk_partner_h',                          models "stage_contact" and "stage_partner". For "stage_account" no 'hk_column' is defined,
                                                          'rsrc_static': '*/SALESFORCE/Partners/*'}}            and therefor the input of the upper level variable 'tracked_hashkey' will be used. 
                                                                                                                For "stage_partner" the name of the hashkey column differs from the upper level definition
                                                                                                                and therefor this other name is set under the variable 'hk_column.'

                                                                                                                The 'rsrc_static' attribute defines a STRING that will be always the same over all
                                                                                                                loads of one source. Something like this needs to be identified for each source system,
                                                                                                                and strongly depends on the actual content of the rsrc column inside the stage.
                                                                                                                Sometimes the rsrc column includes the ldts of each load and could look something
                                                                                                                like this: 'SALESFORCE/Partners/2022-01-01T07:00:00'. Obviously the timestamp part
                                                                                                                inside that rsrc would change from load to load, and we now need to identify parts of
                                                                                                                it that will be static over all loads. Here it would be 'SALESFORCE/Partners'. This static
                                                                                                                part is now enriched by wildcard expressions (in BigQuery that would be '*') to catch
                                                                                                                the variable part of the rsrc values.
                                                                                                                If my rsrc would be the same over all loads, then it might look something like
                                                                                                                this: 'SAP/Accounts/'. Here everything would be static over all loads and
                                                                                                                therefor I would set rsrc_static to 'SAP/Accounts/' without any wildcards in place.

    src_ldts::string                Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                    Needs to use the same column name as defined as alias inside the staging model.

#}

{%- macro rec_track_sat(tracked_hashkey, source_models, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}
    
    {%- set src_ldts = dbtvault_scalefree.replace_standard(src_ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.rsrc_alias', 'rsrc') -%}


    {{ return(adapter.dispatch('rec_track_sat', 'dbtvault_scalefree')(tracked_hashkey=tracked_hashkey,
                                                                      source_models=source_models,
                                                                      src_ldts=src_ldts,
                                                                      src_rsrc=src_rsrc)) }}

{%- endmacro -%}