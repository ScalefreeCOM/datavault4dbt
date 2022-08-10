{#
    This macro creates a Hub entity based on one or more stage models. The macro requires an input source model similar to the output 
    of the dbtvault-scalefree stage macro. So by default the stage models would be used as source models for hubs.

    Parameters:

    hashkey::string                             Name of the hashkey column inside the stage, that should be used as PK of the Hub.

                                                Examples:
                                                    'hk_account_h'      This hashkey column was created before inside the corresponding staging area, using the stage macro.

    business_keys::string|list of strings       Name(s) of the business key columns that should be loaded into the hub and are the input of the hashkey column. Needs to be
                                                available inside the stage model. If the names differ between multiple sources, you should define here how the business keys
                                                should be called inside the final hub model. The actual input column names need to be defined inside the 'source_model'
                                                parameter then. 

                                                Examples: 
                                                    'account_key'                       This hub only has one business key and therefor only one is defined here.

                                                    ['account_key', 'account_number']   This hub has two business keys which are both defined here.
    
    source_models::dictionary                   Dictionary with information about the source models. The keys of the dict are the names of the source models, and the value of each
                                                source model is another dictionary. This inner dictionary requires to have the keys 'rsrc_static', and optionally the keys 'hk_column'
                                                and 'bk_columns'.

                                                Examples: 
                                                    {'stage_account': {'bk_columns': ['account_number', 'account_key'],     This would create a hub loaded from only one source, which is not uncommon.
                                                                    'rsrc_static': '*/SAP/Accounts/*'}}                     It uses the model 'stage_account', and defines the same columns as 'bk_columns'
                                                                                                                            that were used to calculate the hashkey for the hub 'hk_account_h'.

                                                    {'stage_account': {'rsrc_static': '*/SAP/Accounts/*'},                  This would create a hub loaded from two sources, which also is not uncommon.
                                                     'stage_partner': {'bk_columns': 'partner_key',                         It uses the stage model 'stage_account' and since the parameter 'bk_columns'
                                                                       'rsrc_static': '*/SALESFORCE/Partners/*',            is not set, it will use the value defined in the upper level parameter 'business_keys'.
                                                                       'hk_column': 'hk_partner_h'}}                        Additionally the model 'stage_partner' is used, with the assumption that both sources
                                                                                                                            share the same definition of an account, just under different names. Therefor
                                                                                                                            a different business key column is defined as 'bk_columns', but the number of
                                                                                                                            business key columns must be the same over all sources, which is the case here. 
                                                                                                                            The hashkey column inside this stage is called 'hk_partner_h' and is therefor defined
                                                                                                                            under 'hk_column'. If it would not be defined, the macro would always search for
                                                                                                                            a column called similar to the 'hashkey' parameter defined one level above.

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

    src_ldts::string                            Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                            Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                                Needs to use the same column name as defined as alias inside the staging model.

#}


{%- macro hub(hashkey, business_keys, source_models, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}
    
    {%- set src_ldts = dbtvault_scalefree.replace_standard(src_ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.rsrc_alias', 'rsrc') -%}

    {{ return(adapter.dispatch('hub', 'dbtvault_scalefree')(hashkey=hashkey,
                                                            business_keys=business_keys,
                                                            src_ldts=src_ldts,
                                                            src_rsrc=src_rsrc,
                                                            source_models=source_models)) }}

{%- endmacro -%} 