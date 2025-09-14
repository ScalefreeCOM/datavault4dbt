{#
    This macro creates a non-historized (former transactional) link entity, connecting two or more entities, or a transactional fact of one entity. It can be loaded by one or
    more source staging tables, if multiple sources share the same business definitions. If multiple sources are used, it is requried that they all have the same
    number of foreign keys inside, otherwise they would not share the same business definition of that non-historized link.

    In the background a non-historized link uses exactly the same loading logic as a regular link, but adds the descriptive attributes as additional payload.
#}

{%- macro nh_link(yaml_metadata=none, link_hashkey=none, payload=none, source_models=none, foreign_hashkeys=none, src_ldts=none, src_rsrc=none, disable_hwm=false, source_is_single_batch=false, union_strategy='all', additional_columns=none) -%}
    
    {% set link_hashkey_description = "
    link_hashkey::string                    Name of the non-historized link hashkey column inside the stage. Should get calculated out of all business keys inside
                                            the link.

                                            Examples:
                                                'hk_transaction_account_nl'     This hashkey column belongs to the non-historized link between transaction and account, and
                                                                                was created at the staging layer by the stage macro.
    " %}

    {% set foreign_hashkeys_description = "
    foreign_hashkeys::list of strings       List of all hashkey columns inside the non-historized link, that refer to other hub entities. All hashkey columns must
                                            be available inside the stage area.

                                            Examples:
                                                ['hk_transaction_h', 'hk_account_h']    The non-historized link between transaction and account needs to contain both the
                                                                                        hashkey of transaction and account to enable joins to the corresponding hub entities.
    " %}

    {% set payload_description = "
    payload::list of strings                A list of all the descriptive attributes that should be the payload of this non-historized link. If the names differ between source
                                            models, this list will define how the columns are named inside the result non historized link. The mapping which columns to use from
                                            which source model then need to be defined inside the parameter 'payload' inside the variable 'source_models'.

                                            Examples:
                                                ['currency_isocode', 'amount', 'purpose', 'transaction_date']           The non-historized link will be enriched by the descriptive attributes 'currency_isocode',
                                                                                                                        'amount', 'purpose' and 'transaction_date'.
    " %}

    {% set source_models_description = "
    source_models::dictionary               Dictionary with information about the source models. The keys of the dict are the names of the source models, and the value of each
                                            source model is another dictionary. This inner dictionary optionally has the keys 'hk_column',
                                            'fk_columns', 'payload' and 'rsrc_static'.

                                            Examples:
                                                {'stage_account': {'fk_columns': ['hk_account_h', 'hk_contact_h'],      This would create a link loaded from only one source, which is not uncommon.
                                                                   'rsrc_static': '*/SAP/Accounts/*'}}                  It uses the model 'stage_account', and defines the same columns as 'fk_columns'
                                                                                                                        that were defined in the attribute 'foreign_hashkeys'. Therefore it could have
                                                                                                                        been left out here.

                                                {'stage_account': {'rsrc_static': '*/SAP/Accounts/*'},                  This would create a link loaded from two sources, which also is not uncommon.
                                                 'stage_partner': {'fk_columns': ['hk_partner_h', 'hk_customer_h'],     The source model 'stage_account' has no 'fk_columns' and 'link_hk' defined,
                                                                   'rsrc_static': '*/SALESFORCE/Partners/*',            therefore it uses the values set in the upper-level variables 'link_hashkey'
                                                                   'link_hk': 'hk_partner_customer_l',                  and 'foreign_hashkeys'. Additionally the model 'stage_partner' is used, with
                                                                   'payload': ['currency_code', 'amount',               the assumption that both sources share the same definition of an account, just
                                                                               'intended_use', 'date']}}                under different names. therefore a different link hashkey column is defined as
                                                                                                                        'link_hk', but the number of foreign key columns defined in 'fk_columns' must be
                                                                                                                        the same over all sources, which is the case here. 'payload' is also set to names
                                                                                                                        that differ from the payload defined in the upper level. Important here is that the
                                                                                                                        number of columns inside each source models payload definition needs to equal the
                                                                                                                        number of columns defined in the upper level payload definition.

                                                                                                                        The 'rsrc_static' attribute defines a STRING or a list of strings which contains all the patterns
                                                                                                                        of the record_source field that remains the same over the loads of one source.
                                                                                                                        If a list of strings is defined that means that the record_source may have different patterns
                                                                                                                        coming from the same source system. For example a source may be generating files with the pattern
                                                                                                                        '{file_id}-{timestamp}-source.txt' or with the pattern '{file_id}-{timestamp}-{store_id}-source.csv'
                                                                                                                        Those two patterns must be included then in the rsrc_static with the appropriate regex and wildcards in place.
                                                                                                                        If the model has two source models as input and only one source model defines a rsrc_static, then this
                                                                                                                        macro wont use the rsrc_static at all to do the look up in target. Therefore, if there are multiple
                                                                                                                        source models defined, if there is a desire to execute this macro with the performance look up for the
                                                                                                                        rsrc_static, then this parameter has to be defined for every source model that is defined.
                                                                                                                        Something like this needs to be identified for each source system,
                                                                                                                        and strongly depends on the actual content of the record source column inside the stage model.
                                                                                                                        Sometimes the rsrc column includes the ldts of each load and could look something
                                                                                                                        like this: 'SALESFORCE/Partners/2022-01-01T07:00:00'. Obviously the timestamp part
                                                                                                                        inside that rsrc would change from load to load, and we now need to identify parts of
                                                                                                                        it that will be static over all loads. Here it would be 'SALESFORCE/Partners'. This static
                                                                                                                        part is now enriched by wildcard expressions (in BigQuery that would be '*') to catch
                                                                                                                        the variable part of the rsrc values.
                                                                                                                        If the record source is the same over all loads, then it might look something like
                                                                                                                        this: 'SAP/Accounts/'. Here everything would be static over all loads and
                                                                                                                        therefore the rsrc_static would be set to 'SAP/Accounts/' without any wildcards in place.
    " %}

    {% set src_ldts_description = "
    src_ldts::string                        Name of the ldts column inside the source models. Is optional, will use the global variable 'datavault4dbt.ldts_alias'.
                                            Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set src_rsrc_description = "
    src_rsrc::string                        Name of the rsrc column inside the source models. Is optional, will use the global variable 'datavault4dbt.rsrc_alias'.
                                            Needs to use the same column name as defined as alias inside the staging model.
    " %}

    {% set union_strategy_description = "
    union_strategy::'all' | 'distinct'      Defines how multiple sources should be unioned. 'all' will result in a UNION ALL and represents the default value. Should only be changed, if you have duplicates across
                                            source systems, and don't want to deduplicate them upfront. 
    " %}

    {% set additional_columns_description = "
    additional_columns_description::string            Additional columns from source system or derived columns which should be part of NH-Link. Useful when you have to deviate from the normal NH-Link Structure due to organisational or governancance reasons (Multitenant, BKCC, ..). Is optional and as default the normal NH-Link columns are applied.
                                                      Columns needs to be in all source models which are used for the Non-historized Link.
    " %}

    {%- set link_hashkey            = datavault4dbt.yaml_metadata_parser(name='link_hashkey', yaml_metadata=yaml_metadata, parameter=link_hashkey, required=True, documentation=link_hashkey_description) -%}
    {%- set payload                 = datavault4dbt.yaml_metadata_parser(name='payload', yaml_metadata=yaml_metadata, parameter=payload, required=True, documentation=payload_description) -%}
    {%- set source_models           = datavault4dbt.yaml_metadata_parser(name='source_models', yaml_metadata=yaml_metadata, parameter=source_models, required=True, documentation=source_models_description) -%}
    {%- set foreign_hashkeys        = datavault4dbt.yaml_metadata_parser(name='foreign_hashkeys', yaml_metadata=yaml_metadata, parameter=foreign_hashkeys, required=False, documentation=foreign_hashkeys_description) -%}
    {%- set src_ldts                = datavault4dbt.yaml_metadata_parser(name='src_ldts', yaml_metadata=yaml_metadata, parameter=src_ldts, required=False, documentation=src_ldts_description) -%}
    {%- set rsrc                    = datavault4dbt.yaml_metadata_parser(name='rsrc', yaml_metadata=yaml_metadata, parameter=rsrc, required=False, documentation=rsrc_description) -%}
    {%- set disable_hwm             = datavault4dbt.yaml_metadata_parser(name='disable_hwm', yaml_metadata=yaml_metadata, parameter=disable_hwm, required=False, documentation='Whether the High Water Mark should be turned off. Optional, default False.') -%}
    {%- set source_is_single_batch  = datavault4dbt.yaml_metadata_parser(name='source_is_single_batch', yaml_metadata=yaml_metadata, parameter=source_is_single_batch, required=False, documentation='Whether the source contains only one batch. Optional, default False.') -%}
    {%- set union_strategy =  datavault4dbt.yaml_metadata_parser(name='union_strategy', yaml_metadata=yaml_metadata, parameter=union_strategy, required=False, documentation=union_strategy_description) -%}
    {%- set additional_columns     = datavault4dbt.yaml_metadata_parser(name='additional_columns', yaml_metadata=yaml_metadata, parameter=additional_columns, required=False, documentation=additional_columns_description) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{- adapter.dispatch('nh_link', 'datavault4dbt')(link_hashkey=link_hashkey,
                                                        payload=payload,
                                                        foreign_hashkeys=foreign_hashkeys,
                                                        src_ldts=src_ldts,
                                                        src_rsrc=src_rsrc,
                                                        source_models=source_models,
                                                        disable_hwm=disable_hwm,
                                                        source_is_single_batch=source_is_single_batch,
                                                        union_strategy=union_strategy,
                                                        additional_columns=additional_columns) -}}

{%- endmacro -%}
