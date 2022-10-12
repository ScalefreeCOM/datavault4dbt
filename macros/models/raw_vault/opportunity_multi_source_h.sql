{{ config(schema='public_release_test',
           materialized='incremental') }}



{{ datavault4dbt.hub(hashkey='hk_opportunity_h',
                 source_models={'stage_opportunity': {'bk_columns': 'opportunity_key__c',
                                                    'rsrc_static': '*/SALESFORCE/06sIPY/Opportunity/*'},
                                'stage_account': {'bk_columns': 'account_key__c',
                                                    'hk_column': 'hk_account_h',
                                                    rsrc_all_static: false} },
                 src_ldts='ldts',
                 src_rsrc='rsrc') }}

{{ datavault4dbt.hub(hashkey='hk_opportunity_h',
                 source_models={'stage_opportunity': {'bk_columns': 'opportunity_key__c',
                                                    'rsrc_static': '*/SALESFORCE/06sIPY/Opportunity/*'},
                                'stage_account': {'bk_columns': 'account_key__c',
                                                    'hk_column': 'hk_account_h',
                                                    rsrc_is_static: true} },
                 src_ldts='ldts',
                 src_rsrc='rsrc') }}


{{ datavault4dbt.hub(hashkey='hk_opportunity_h',
                 source_models={'stage_opportunity': {'bk_columns': 'opportunity_key__c',
                                                    'rsrc_static': '*/SALESFORCE/06sIPY/Opportunity/*'},
                                'stage_account': {'bk_columns': 'account_key__c',
                                                    'hk_column': 'hk_account_h',
                                                    'rsrc_static': '*'} }, --- either rsrc is completely static or nothing in it is static
                 src_ldts='ldts',
                 src_rsrc='rsrc') }}
