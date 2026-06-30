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
| source_model   | string         | mandatory | –             | Name of the underlying staging model, must be available inside dbt as a model. |

### OPTIONAL PARAMETERS

| Parameters             | Data Type | Required | Default Value             | Explanation |
|------------------------|----------|----------|---------------------------|-------------|
| src_payload            | list of strings | optional | none               | The descriptive attributes that should be included in this satellite. With a single column, `src_hashdiff` may be omitted and change detection runs directly on that column. With two or more columns, `src_hashdiff` is required. Omit entirely for a satellite without payload. See [Payload and hashdiff options](#payload-and-hashdiff-options). |
| src_hashdiff           | string   | optional | none                      | Name of the hashdiff column of this satellite, that was created inside the staging area and is calculated out of the entire payload of this satellite. The stage must hold one hashdiff per satellite entity. Required when `src_payload` has two or more columns; omit it for a single-attribute or no-payload satellite. |
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

## PAYLOAD AND HASHDIFF OPTIONS

Both `src_payload` and `src_hashdiff` are optional. Depending on what you provide, a standard satellite supports three configurations:

1. **Multiple attributes with a hashdiff** — provide `src_payload` (one or more columns) together with `src_hashdiff`. Change detection runs on the hashdiff. *Use this when the satellite carries several descriptive attributes — the classic case shown in Example 1 above.*
2. **A single attribute without a hashdiff** — provide exactly one `src_payload` column and omit `src_hashdiff`. Change detection runs directly on that column. *Use this when the satellite tracks exactly one descriptive attribute (for example a status or a flag); it avoids computing and storing a hashdiff.*
3. **No payload** — omit both `src_payload` and `src_hashdiff`. The satellite only records when a hashkey appears, together with the load date, record source and any `additional_columns`. *Use this when you only need to record that (and when) a business key appeared, or for a satellite that carries only `additional_columns`.*

If `src_payload` contains two or more columns but `src_hashdiff` is missing, compilation fails with a clear error, since a hashdiff is required to detect changes across multiple columns.

### EXAMPLE 2 – single attribute without a hashdiff

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_account_h'
src_payload:
    - account_status
source_model: 'stage_account'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
```

Change detection runs directly on `account_status`; a new record is inserted whenever its value changes for a hashkey.

### EXAMPLE 3 – no payload

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_account_h'
source_model: 'stage_account'
{%- endset -%}    

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
```

The satellite records each hashkey together with its load date and record source. A new record is only inserted when a hashkey appears that is not yet present. If your goal is purely to track the appearance of a key across one or more source systems, also consider the dedicated [Record-Tracking Satellite](../15_record-tracking-satellites/15_record-tracking-satellites.md).