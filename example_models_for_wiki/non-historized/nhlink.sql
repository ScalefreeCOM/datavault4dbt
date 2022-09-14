{{ config(materialized='incremental',
          unique_key='hk_creditcard_transactions_nl') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_creditcard_transactions_nl'
foreign_hashkeys: 
    - 'hk_creditcard_h'
payload:
    - transactionid
    - amount
    - currency_code
    - is_canceled
    - transaction_date
source_models:
    stage_creditcard_transactions:
        rsrc_static: '*/VISA/Transactions/*'
    stage_purchases:
        link_hk: 'transaction_hashkey'
        fk_columns: ['creditcard_hkey']
        payload: 
            - id
            - amount_CUR
            - currency
            - status_flag
            - date
        rsrc_static: '*/SHOP/Creditcard_Purchases/*'
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set link_hashkey = metadata_dict['link_hashkey'] -%}
{%- set foreign_hashkeys = metadata_dict['foreign_hashkeys'] -%}
{%- set payload = metadata_dict['payload'] -%}
{%- set source_models = metadata_dict['source_models'] -%}


{{ datavault4dbt.nh_link(link_hashkey=link_hashkey,
                         foreign_hashkeys=foreign_hashkeys,
                         payload=payload,
                         source_models=source_models) }}
---------------------------------------------------------------------------
Description from macro file. point out, that foreign hashkeys can also only contain one hk. Multi Source NHLink.

     