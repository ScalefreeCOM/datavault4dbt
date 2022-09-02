{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
source_models:
    stage_account:
        rsrc_static: '*/SAP/Accounts/*'
    stage_partner:
        hk_column: 'hk_partner_h'
        bk_columns:
            - partner_key
            - partner_number
        rsrc_static: '*/SALESFORCE/Partners/*'
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set hashkey = metadata_dict['hashkey'] -%}
{%- set business_keys = metadata_dict['business_keys'] -%}
{%- set source_models = metadata_dict['source_models'] -%}

{{ datavault4dbt.hub(hashkey=hashkey,
                     business_keys=business_keys,
                     source_models=source_models) }}
------------------------------------------------------------
Description:

hashkey:
    This hashkey column was created before inside the corresponding staging area, using the stage macro.

business_keys:
    This hub has two business keys which are both defined here. Need to equal the input columns for the hashkey column.

source_models:
    This would create a hub loaded from two sources, which also is not uncommon.
    It uses the stage model 'stage_account' and since the parameter 'bk_columns'
    is not set, it will use the value defined in the upper level parameter 'business_keys'.
    Additionally the model 'stage_partner' is used, with the assumption that both sources
    share the same definition of an account, just under different names. Therefor
    a different business key column is defined as 'bk_columns', but the number of
    business key columns must be the same over all sources, which is the case here. 
    The hashkey column inside this stage is called 'hk_partner_h' and is therefor defined
    under 'hk_column'. If it would not be defined, the macro would always search for
    a column called similar to the 'hashkey' parameter defined one level above.

    The static part of the record source column inside 'stage_partner' is '*/SALESFORCE/Partners/*'.