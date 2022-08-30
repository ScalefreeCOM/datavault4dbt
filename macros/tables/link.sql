{#
    This macro creates a link entity, connecting two or more entities, or an entity with itself. It can be loaded by one or more source staging tables,
    if multiple sources share the same buisness definitions. Typically a link would only be loaded by multiple sources, if those multiple sources also
    share the business defintions of the hubs, and therefor load the connected hubs together aswell. If multiple sources are used, it is requried that they
    all have the same number of foreign keys inside, otherwise they would not share the same business definition of that link.

    Features:
        - Loadable by multiple sources
        - Supports multiple updates per batch and therefor initial loading
        - Using a dynamic high-water-mark to optimize loading performance of multiple loads
        - Allows source mappings for deviations between source column names and hub column names

    Parameters: 

    link_hashkey::string                    Name of the link hashkey column inside the stage. Should got calculated out of all business keys inside
                                            the link.

                                            Examples:
                                                'hk_account_contact_l'      This hashkey column belongs to the link between account and contact, and
                                                                            was created at the staging layer by the stage macro.

    foreign_hashkeys::list of strings       List of all hashkey columns inside the link, that refer to other hub entities. All hashkey columns must
                                            be available inside the stage area.

                                            Examples:
                                                ['hk_account_h', 'hk_contact_h']    The link between account and contact needs to containt both the
                                                                                    hashkey of account and contact to enable joins the the corresponding
                                                                                    hub entities.

    source_models::dictionary               Dictionary with information about the source models. The keys of the dict are the names of the source models, and the value of each
                                            source model is another dictionary. This inner dictionary requires to have the keys 'rsrc_static', and optionally the keys 'hk_column'
                                            and 'fk_columns'.

                                            Examples:
                                                {'stage_account': {'fk_columns': ['hk_account_h', 'hk_contact_h'],      This would create a link loaded from only one source, which is not uncommon.
                                                                   'rsrc_static': '*/SAP/Accounts/*'}}                  It uses the model 'stage_account', and defines the same columns as 'fk_columns'
                                                                                                                        that were defined in the attribute 'foreign_hashkeys'. Therefor it could have
                                                                                                                        been left out here.

                                                {'stage_account': {'rsrc_static': '*/SAP/Accounts/*'},                  This would create a link loaded from two sources, which also is not uncommon.
                                                 'stage_partner': {'fk_columns': ['hk_partner_h', 'hk_customer_h'],     The source model 'stage_account' has no 'fk_columns' and 'link_hk' defined,
                                                                   'rsrc_static': '*/SALESFORCE/Partners/*',            therefor it uses the values set in the upper-level variables 'link_hashkey'
                                                                   'link_hk': 'hk_partner_customer_l'}}                 and 'foreign_hashkeys'. Aditionally the model 'stage_partner' is used, with
                                                                                                                        the assumption that both sources share the same definition of an account, just
                                                                                                                        under different names. Therefor a different link hashkey column is defined as
                                                                                                                        'link_hk', but the number of foreign key columns defined in 'fk_columns' must be
                                                                                                                        the same over all sources, which is the case here.

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

    src_ldts::string                        Name of the ldts column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.ldts_alias'.
                                            Needs to use the same column name as defined as alias inside the staging model.

    src_rsrc::string                        Name of the rsrc column inside the source models. Is optional, will use the global variable 'dbtvault_scalefree.rsrc_alias'.
                                            Needs to use the same column name as defined as alias inside the staging model.

#}

{%- macro link(link_hashkey, foreign_hashkeys, source_models, src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = dbtvault_scalefree.replace_standard(src_ldts, 'dbtvault_scalefree.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = dbtvault_scalefree.replace_standard(src_rsrc, 'dbtvault_scalefree.rsrc_alias', 'rsrc') -%}

    {{- adapter.dispatch('link', 'dbtvault_scalefree')(link_hashkey=link_hashkey, foreign_hashkeys=foreign_hashkeys,
                                             src_ldts=src_ldts, src_rsrc=src_rsrc,
                                             source_models=source_models) -}}

{%- endmacro -%}
