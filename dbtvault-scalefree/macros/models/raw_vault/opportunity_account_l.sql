{{ config(schema='public_release_test',
           materialized='incremental') }}

{{ link(source_model='stage_opportunity',
        src_pk='hk_opportunity_account_l',
        src_fk=['hk_opportunity_h', 'hk_account_h']
        )}}