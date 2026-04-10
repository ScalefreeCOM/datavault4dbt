---
sidebar_position: 10
sidebar_label: Standard Satellite v0
title: Standard Satellite v0
---

# STANDARD SATELLITE V0

---

This macro creates a standard satellite version 0, meaning that it should be materialized as an incremental table. It should be applied `on top` of the staging layer, and is either connected to a Hub or a Link. On top of each version 0 satellite, a version 1 satellite should be created, using the sat_v1 macro. This extends the v0 satellite by a virtually calculated load end date. Each satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.

Features:

- Can handle multiple updates per batch, without losing intermediate changes. Therefore initial loading is supported.
- Using a dynamic high-water-mark to optimize loading performance of multiple loads

### REQUIRED PARAMETERS

| Parameters      | Data Type       | Required  | Default Value | Explanation |
|-----------------|----------------|-----------|---------------|-------------|
| parent_hashkey | string         | mandatory | –             | Name of the hashkey column inside the stage of the object that this satellite is attached to. |
| src_hashdiff   | string         | mandatory | –             | Name of the hashdiff column of this satellite, that was created inside the staging area and is calculated out of the entire payload of this satellite. The stage must hold one hashdiff per satellite entity. |
| src_payload    | list of strings| mandatory | –             | A list of all the descriptive attributes that should be included in this satellite. Needs to be the columns that are fed into the hashdiff calculation of this satellite. |
| source_model   | string         | mandatory | –             | Name of the underlying staging model, must be available inside dbt as a model. |

### OPTIONAL PARAMETERS

| Parameters             | Data Type | Required | Default Value             | Explanation |
|------------------------|----------|----------|---------------------------|-------------|
| src_ldts               | string   | optional | datavault4dbt.ldts_alias  | Name of the ldts column inside the source model. Is optional, will use the global variable `datavault4dbt.ldts_alias`. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc               | string   | optional | datavault4dbt.rsrc_alias  | Name of the rsrc column inside the source model. Is optional, will use the global variable `datavault4dbt.rsrc_alias`. Needs to use the same column name as defined as alias inside the staging model. |
| disable_hwm            | boolean  | optional | False                     | Whether the automatic application of a High-Water Mark (HWM) should be disabled or not. |
| source_is_single_batch | boolean  | optional | False                     | Boosts performance by disabling QUALIFY statement. Only activate this if you make sure that underlying staging model only holds one row per entry. See non-historized link for more details. |
| additional_columns     | string \| list of strings | optional | none                      | Column or list of columns that will additionally be added to the satellite. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_account_h'
src_hashdiff: 'hd_account_s'
src_payload:
    - name
    - address
    - phone
    - email    
source_model: 'stage_account'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a regular standard Satellite in version 0 is created. Important is that the payload needs to contain the exact same attributes as the corresponding hashdiff column, which was created in the staging area.

- **parent_hashkey**:
  - __hk_account_h__: The satellite would be attached to the hub account, which has the column `hk_account_h` as a hashkey column.
- **src_hashdiff**:
  - __hd_account_s__: Since we recommend naming the hashdiff column similar to the name of the satellite entity, just with a prefix, this would be the hashdiff column of the data satellite for account.
- **src_payload**: This satellite would hold the columns `name`, `address`, `phone` and `email`, coming out of the underlying staging area.
- **source_models**:
  - __stage_account__: This satellite is loaded out of the stage for account.