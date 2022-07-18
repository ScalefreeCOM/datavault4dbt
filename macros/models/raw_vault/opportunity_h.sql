{{ config(schema='public_release_test',
           materialized='incremental') }}

{{ hub(hashkey='hk_opportunity_h',
                 business_key=['opportunity_key__c'],
                 src_ldts='ldts',
                 src_rsrc='rsrc',
                 source_model='stage_opportunity') }}