---
sidebar_position: 19
sidebar_label: Reference Satellite v0
title: Reference Satellite v0
---

# REFERENCE SATELLITE V0

---

This macro creates a reference satellite version 0, meaning that it should be materialized as an incremental table. It should be applied `on top` of the staging layer, and is connected to a reference Hub. On top of each version 0 reference satellite, a version 1 reference satellite should be created, using the ref_sat_v1 macro. This extends the v0 reference satellite by a virtually calculated load end date. Each reference satellite can only be loaded by one source model, since we typically recommend a satellite split by source system.

Compared to a regular satellite, a reference Satellite does not hold a Hashkey of the parent Hub, since the parent reference Hub does not have a Hub Hashkey. Instead, all reference keys of the parent reference Hub are loaded in the reference Satellite to allow unique connections between them.

Besides that, a reference Satellite v0 shares the same features as a regular Satellite v0, which are:

- Can handle multiple updates per batch, without losing intermediate changes. Therefore initial loading is supported.
- Using a dynamic high-water-mark to optimize loading performance of multiple loads

### REQUIRED PARAMETERS

| Parameters      | Data Type        | Required  | Default Value | Explanation |
|-----------------|------------------|-----------|---------------|-------------|
| parent_ref_keys | string \| list   | mandatory | –             | Name of the reference key(s) inside the parent reference Hub. |
| src_hashdiff    | string           | mandatory | –             | Name of the hashdiff column of this satellite, that was created inside the staging area and is calculated out of the entire payload of this satellite. The stage must hold one hashdiff per satellite entity. |
| src_payload     | list of strings  | mandatory | –             | A list of all the descriptive attributes that should be included in this satellite. Needs to be the columns that are fed into the hashdiff calculation of this satellite. |
| source_model    | string           | mandatory | –             | Name of the underlying staging model, must be available inside dbt as a model. |

### OPTIONAL PARAMETERS

| Parameters  | Data Type | Required | Default Value              | Explanation |
|-------------|-----------|----------|----------------------------|-------------|
| src_ldts    | string    | optional | datavault4dbt.ldts_alias   | Name of the ldts column inside the source model. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc    | string    | optional | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source model. Is optional, will use the global variable `datavault4dbt.rsrc_alias`. Needs to use the same column name as defined as alias inside the staging model. |
| disable_hwm | boolean   | optional | False                      | Whether the automatic application of a High-Water Mark (HWM) should be disabled or not. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental',
        schema='Core') }}

{%- set yaml_metadata -%}
source_model: stg_nation
parent_ref_keys: N_NATIONKEY
src_hashdiff: hd_nation_rs
src_payload:
    - N_COMMENT
    - N_NAME
    - N_REGIONKEY
{%- endset -%}      

{{ datavault4dbt.ref_sat_v0(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **parent_ref_keys**:
  - __N_NATIONKEY__: This reference Satellite would be attached to the reference Hub “Nation_rh” and therefore it must hold all the reference Keys of that Hub. In this case it would only be the column “N_NATIONKEY”
- **src_hashdiff**:
  - __hd_nation_rs__: Since we recommend naming the hashdiff column similar to the name of the satellite entity, just with a prefix, this would be the hashdiff column of the reference satellite for Nation.
- **src_payload**: This satellite would hold the columns `N_COMMENT`, `N_NAME` and `N_REGIONKEY`, coming out of the underlying staging area. Must fit the input definition for the specified hashdiff column.
- **source_models**:
  - __stg_nation__: This satellite is loaded out of the stage for nation.

### COMPILED SQL

<details>
  <summary>Click me</summary>
```sql
WITH


source_data AS (

    SELECT
        
        N_NATIONKEY,
        
        hd_nation_rs as hd_nation_rs,
        
        rsrc,
        ldts,
        N_COMMENT,
        N_NAME,
        N_REGIONKEY
    FROM datavault4dbt_demo.core_Stages.stg_nation
),




deduplicated_numbered_source AS (

    SELECT
    
    N_NATIONKEY,
    
    hd_nation_rs,
    
        rsrc,
        ldts,
        N_COMMENT,
        N_NAME,
        N_REGIONKEY
    
    FROM source_data
    QUALIFY
        CASE
            WHEN hd_nation_rs = LAG(hd_nation_rs) OVER(PARTITION BY N_NATIONKEY ORDER BY ldts) THEN FALSE
            ELSE TRUE
        END
),


records_to_insert AS (

    SELECT
    
    N_NATIONKEY,
    
    hd_nation_rs,
    
        rsrc,
        ldts,
        N_COMMENT,
        N_NAME,
        N_REGIONKEY
    FROM deduplicated_numbered_source

    )

SELECT * FROM records_to_insert
```
</details>