{{ config(schema='public_release_test',
           materialized='incremental') }}

{{ link(source_model='stage_opportunity',
        link_hashkey='hk_opportunity_account_l',
        foreign_hashkeys=['hk_opportunity_h', 'hk_account_h']
        )}}