{{ config(schema='public_release_test',
           materialized='incremental') }}

{{ hub(hashkey='hk_opportunity_h',
                 src_ldts='ldts',
                 src_rsrc='rsrc',
                 source_model={'stage_opportunity': {'bk_columns': 'opportunity_key__c',
                                                    'rsrc_static': '*/SALESFORCE/06sIPY/Opportunity/*'}}) }}
