{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
source_models:
    stage_account:
        rsrc_static: '*/SAP/Accounts/*'
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
    This would create a hub loaded from only one source, which is not uncommon.
    It uses the model 'stage_account' and since no 'bk_columns' are specified, the same
    columns as defined in 'business_keys' will be selected from the source.

    The 'rsrc_static' attribute defines a STRING that will be always the same over all
    loads of one source. Something like this needs to be identified for each source system,
    and strongly depends on the actual content of the rsrc column inside the stage.
    Sometimes the rsrc column includes the ldts of each load and could look something
    like this: 'SAP/Accounts/2022-01-01T07:00:00'. Obviously the timestamp part
    inside that rsrc would change from load to load, and we now need to identify parts of
    it that will be static over all loads. Here it would be 'SAP/Accounts'. This static
    part is now enriched by wildcard expressions (in BigQuery that would be '*') to catch
    the variable part of the rsrc values.