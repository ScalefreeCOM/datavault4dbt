---
sidebar_position: 5
sidebar_label: Add new columns To Hashdiff
title: Add new columns To Hashdiff
---

# ADD NEW COLUMNS TO HASHDIFF

---

DISCLAIMER: This feature was available for Snowflake in the versions from v1.1.4 up to v1.2.2. It will become available for all adapters from v2.0.0 again.

Did your source add new columns and you want to add them to the same satellite you held the other columns of this source? Don`t worry! By using `append_new_column` as value for the `on_schema_change` config variable in dbt it is possible to append new columns to a Satellite. For that you would also have to add these new columns to the `src_payload` key in the Satellite v0 model and to the list in the `columns` key inside the definition of your satellite`s hashdiff in the stage model.

When a new column(s) is(are) added in a Satellite and also added to the Hashdiff list of columns, the calculated Hashdiff in the Stage normally would be different to those already present in the Satellite even if the new column(s) is(are) NULL. This could result in many unnecessary new rows being added into the satellite. We have developed a solution for the Snowflake datavault4dbt adapter, where you can set in the Stage model inside the Satellite Hashdiff definition the key value pair: `use_rtrim:true`.

By adding this optional key to your stage you can now use the dbt config variables to avoid inserting a new row when all the new columns you have added to the satellite still hold NULL values.

Note that if your Satellite already has the last columns (the ones first inserted in the satellite) holding NULL values, and you add new columns that also hold NULL values, and you were **not** previously using the key value `use_rtrim:true` in the hashdiff but started using now, it will still calculate a different Hashdiff then the ones loaded previously in the Satellite. This is because we use delimiters that replace NULL value in the hash calculation, and they will only get removed from the value to be hashed if you use `use_rtrim:true`, and since your Satellite was previously loaded with the hashdiff calculation that does not remove these delimiters before hashing the hashdiffs results will still be different. We recommend from now on if you use Snowflake adapter always using `use_rtrim:true` in the hashdiffs of newly added Satellites to avoid this mentioned issue if your satellite may in the future need to be extended to accommodate new columns.

## EXAMPLE 1 - STAGE DEFINTION

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: 'source_account'
ldts: 'edwLoadDate'
rsrc: 'edwRecordSource'
hashed_columns: 
    hk_account_h:
        - account_number
        - account_key
    hd_account_s:
        is_hashdiff: true
        use_rtrim: true
        columns:
            - name
            - address
            - phone
            - email
            - new_col1
            - new_col2
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
```

## EXAMPLE 1 - SATELLITE V0 DEFINITION

Note that you have to add the `on_schema_change` key with the value `append_new_columns` to your config clause in the Satellite v0 model. In the `src_payload` just add the new columns **after** the already present columns (this is important because we trim the hashdiff on the right). In this example below, the columns `name`, `address`, `phone`, `email` were already in the satellite and we added the columns `new_col1` and `new_col2`

```jinja
{{ config(materialized='incremental', on_schema_change="append_new_columns") }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_account_h'
src_hashdiff: 'hd_account_s'
src_payload:
    - name
    - address
    - phone
    - email
    - new_col1
    - new_col2    
source_model: 'stage_account'
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
```