{#
    This macro creates a Hub entity based on one or more stage models. The macro requires an input source model similar to the output
    of the dbtvault-scalefree stage macro. So by default the stage models would be used as source models for hubs.

    Features:
        - Loadable by multiple sources
        - Supports multiple updates per batch and therefor initial loading
        - Can use a dynamic high-water-mark to optimize loading performance of multiple loads
        - Allows source mappings for deviations between source column names and hub column names
        
    Parameters:

    hashkey::string             Name of the hashkey column inside the stage, that should be used as PK of the Hub.

                                Examples:
                                    'hk_account_h'      This hashkey column was created before inside the corresponding staging area, using the stage macro.

    source_models::dictionary   Dictionary with information about the source models. The keys of the dict are the names of the source models, and the value of each
                                source model is another dictionary. This inner dictionary requires the key 'bk_columns' to be set (which contains the name of the business keys of that source model),
                                and can have the optional keys 'hk_column', 'rsrc_static'.

                                Examples:
                                    {'stage_account': {'bk_columns': ['account_number', 'account_key'],     This would create a hub loaded from only one source, which is not uncommon.
                                                       'rsrc_static': '*/SAP/Accounts/*'}}                  It uses the model 'stage_account', and defines the same columns as 'bk_columns'
                                                                                                            that were used to calculate the hashkey for the hub 'hk_account_h'.

                                    {'stage_account': {'bk_columns': 'account_key',                         This would create a hub loaded from two sources, which also is not uncommon.
                                                       'rsrc_static': '*/SAP/Accounts/*'},                  It again uses the model 'stage_account' but in this case the hashkey 'hk_account_h'
                                     'stage_partner': {'bk_columns': 'partner_key',                         inside it was only calculated out of one business key. Therefor you see that
                                                       'rsrc_static': '*/SALESFORCE/Partners/*',            'bk_columns' can either be a string or a list. It just have to match exactly the
                                                       'hk_column': 'hk_partner_h'}}                        attributes that were used to calculate the hashkey inside that source.
                                                                                                            Additionally the model 'stage_partner' is used, with the assumption that both sources
                                                                                                            share the same definition of an account, just under different names. Therefor
                                                                                                            a different business key column is defined as 'bk_columns', but the number of
                                                                                                            business key columns must be the same over all sources, which is the case here.
                                                                                                            The hashkey column inside this stage is called 'hk_partner_h' and is therefor defined
                                                                                                            under 'hk_column'. If it would not be defined, the macro would always search for
                                                                                                            a column called similar to the 'hashkey' parameter defined one level above.

                                                                                                            The 'rsrc_static' attribute defines a STRING or a list of strings which contains the part
                                                                                                            of the record_source field that remains the same over the loads of one source.
                                                                                                            If a list of strings is defined that means that the record_source may have different patterns
                                                                                                            coming from the same source system. For example a source may be generating files with the pattern
                                                                                                            '{file_id}-{timestamp}-source.txt' or with the pattern '{file_id}-{timestamp}-{store_id}-source.csv'
                                                                                                            Those two patterns must be included then in the rsrc_static with the appropriate regex and wildcards in place.
                                                                                                            If the model has two source models as input and only one source model defines a rsrc_static, then this
                                                                                                            macro won't use the rsrc_static at all to do the look up in target. Therefore, if there are multiple
                                                                                                            source models defined, if there is a desire to execute this macro with the performance look up for the
                                                                                                            rsrc_static, then this parameter has to be defined for every source model that is defined. Something like this needs to be identified for each source system,
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

    src_ldts::string            Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string            Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                Needs to use the same column name as defined as alias inside the staging model.

#}


{%- macro hub(hashkey, source_models, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- if src_ldts is none -%}
        {%- set src_ldts = var('dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- endif -%}

    {%- if src_rsrc is none -%}
        {%- set src_rsrc = var('dbtvault_scalefree.rsrc_alias', 'rsrc') -%}
    {%- endif -%}

    {{ return(adapter.dispatch('hub', 'dbtvault_scalefree')(hashkey=hashkey,
                                                  src_ldts=src_ldts,
                                                  src_rsrc=src_rsrc,
                                                  source_models=source_models)) }}

{%- endmacro -%}
