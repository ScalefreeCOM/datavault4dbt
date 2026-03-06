---
sidebar_position: 12
sidebar_label: Multi-Active Satellite v0
title: Multi-Active Satellite v0
---

# MULTI-ACTIVE SATELLITE V0

---

This macro creates a multi-active satellite version 0, meaning that it should be materialized as an incremental table. It should be applied `on top` of the staging layer, and is either connected to a Hub or a Link. On top of each version 0 multi-active satellite, a version 1 multi-active satellite should be created, using the ma_sat_v1 macro. This extends the v0 satellite by a virtually calculated load end date. Each satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.

If a stage model is defined as multi-active, all satellites out of that stage model need to be implemented as multi-active satellites.

Features:

- Can handle multiple updates per batch, without losing intermediate changes. Therefore initial loading is supported.
- Using a dynamic high-water-mark to optimize loading performance of multiple loads

### REQUIRED PARAMETERS

| Parameters     | Data Type                | Required  | Default Value | Explanation |
|----------------|--------------------------|-----------|---------------|-------------|
| parent_hashkey | string                   | mandatory | –             | Name of the hashkey column inside the stage of the object that this satellite is attached to. |
| src_hashdiff   | string                   | mandatory | –             | Name of the hashdiff column of this satellite, that was created inside the staging area and is calculated out of the entire payload of this satellite. The stage must hold one hashdiff per satellite entity. |
| src_ma_key     | string \| list of strings | mandatory | –             | Name(s) of the multi-active keys inside the staging area. Need to be the same ones, as defined in the stage model. |
| src_payload    | list of strings          | mandatory | –             | A list of all the descriptive attributes that should be included in this satellite. Needs to be the columns that are used for the hashdiff calculation of this satellite. Do not include the multi-active key in the payload of a multi-active satellite, it is included automatically! |
| source_model   | string                   | mandatory | –             | Name of the underlying staging model, must be available inside dbt as a model. |

### OPTIONAL PARAMETERS

| Parameters | Data Type | Required | Default Value             | Explanation |
|------------|-----------|----------|---------------------------|-------------|
| src_ldts   | string    | optional | datavault4dbt.ldts_alias  | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc   | string    | optional | datavault4dbt.rsrc_alias  | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
source_model: 'stg_customer'
parent_hashkey: 'hk_customer_h'
src_hashdiff: 'hd_customer_s'
src_ma_key: 'ma_attribute'
src_payload: 
    - phonenumber
    - address
{%- endset -%}

{{ datavault4dbt.ma_sat_v0(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **source_model**:
  - __stg_customer__: This satellite is created out of the stage for customer data. The stage must be set up as a multi active stage to enable proper hashdiff calculation.
- **parent_hashkey**:
  - __hk_customer_h__: The multi active satellite is attached to the main business object of stg_customer, which is the Hub customer. The hashkey of that hub is hk_customer_h.
- **src_hashdiff**:
  - __hd_customer_s__: The hashdiff column inside the staging model that belongs to this satellite. Needs to have the same input attributes as the payload of this satellite.
- **src_ma_key**:
  - __`ma_attribute`__: For each hashkey and load date, there are multiple ma_attributes that are active at the same time. The combination of hashkey and ma_attribute needs to be unique per ldts.
- **src_payload**:
  - __[`phonenumber`, `address`]__: The multi active satellite for customers needs to contain all descriptive attributes that belong to customers. Need to be the same columns as used for the hashdiff calculation of this satellite.