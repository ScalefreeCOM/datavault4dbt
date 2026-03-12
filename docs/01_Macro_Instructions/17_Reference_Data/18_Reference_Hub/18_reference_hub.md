---
sidebar_position: 18
sidebar_label: Reference Hub
title: Reference Hub
---

# REFERENCE HUB

---

Compared to a Standard Hub, a Reference Hub is created to store [reference Data](/docs/Macro_Instructions/Reference_Data/). The source model of a reference Hub would be a [stage model](/docs/Macro_Instructions/Staging/), but compared to a standard Hub, there is no Hub Hashkey required. Instead, a reference Hub only contains the unhashed one or multiple reference keys.

If a reference Hub is loaded from multiple sources, each source is required to have the same number of reference keys. Additionally each source needs to have a [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) defined.

In general, a reference Hub shares the same features as a general standard Hub, which are:

- Loadable by multiple sources
- Supports multiple updates per batch and therefore initial loading
- Using a dynamic high-water-mark to optimize loading performance of multiple loads
- Allows source mappings for deviations between source column names and hub column names

### REQUIRED PARAMETERS

| Parameter     | Data type                                   | Required  | Default Value | Explanation |
|---------------|---------------------------------------------|-----------|---------------|-------------|
| ref_keys      | string \| list of strings                   | mandatory | –             | Name of the reference key(s) inside the source system. If multiple keys are used, then the ref_keys need to be given as a list of strings. |
| source_models | string \| list of dictionaries \| dictionary | mandatory | –             | If single source, just a string holding the name of the stage model is required. For multi source reference Hubs, a list of dictionaries with information of each source is required. Please see [this](/docs/General_Usage_Notes/Multi-Source_Entities/) page for more details. The inner dictionaries must have `name` as a key, and optionally the keys `rsrc_static` & `ref_keys`. For further information about the rsrc_static attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) |

### OPTIONAL PARAMETERS

| Parameter | Data type | Required | Default Value              | Explanation |
|-----------|----------|----------|----------------------------|-------------|
| src_ldts  | string   | optional | datavault4dbt.ldts_alias   | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc  | string   | optional | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| additional_columns | string \| list of strings | optional | none                       | Column or list of columns that will additionally be added to the reference hub. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental',
          schema='Core') }}

{%- set yaml_metadata -%}
source_models: stg_nation
ref_keys: N_NATIONKEY
{%- endset -%}      

{{ datavault4dbt.ref_hub(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **ref_keys**: This source model has one reference key, the column “N_NATIONKEY”.
- **source_models**: This would create a reference Hub loaded from only one source. It uses the model `stg_nation` and it is not defined as a dictionary because the parameters for this source (only the ref_keys) match the higher-level definition.
  - The __`rsrc_static`__ attribute is not set, because it is not required for single source entities. For more information see [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute).

### COMPILED SQL

<details>
  <summary>Click me</summary>
```sql
WITH

src_new_1 AS (

        SELECT
            N_NATIONKEY,
            ldts,
            rsrc
        FROM datavault4dbt_demo.core_Stages.stg_nation src

    ),

earliest_ref_key_over_all_sources AS (
    SELECT
        lcte.*
    FROM src_new_1 AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY ldts) = 1),

records_to_insert AS (
    SELECT
        
        N_NATIONKEY,
        ldts,
        rsrc
    FROM earliest_ref_key_over_all_sources)

SELECT * FROM records_to_insert  
```
</details>

## EXAMPLE 2

```jinja
{{ config(materialized='incremental',
        schema='Core') }}

{%- set yaml_metadata -%}
source_models: 
    - name: stg_nation
      rsrc_static: 'TPC_H_SF1.Nation'
    - name: stg_customers
      ref_keys: C_NATIONKEY
      rsrc_static: 'TPC_H_SF1.Customer'
ref_keys: N_NATIONKEY
{%- endset -%}      

{{ datavault4dbt.ref_hub(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **ref_keys**: This source model only has one reference key, the column “N_NATIONKEY”.
- **source_models**: This would create a reference Hub loaded from two different sources. From the model `stg_nation` it will select the column `N_NATIONKEY` as the reference key, because there is not source-specific definition for this parameter. For the model `stg_customers` it will select the column `C_NATIONKEY` as defined.
  - The __`rsrc_static`__ attribute is set for each source. For more information see [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute).

### COMPILED SQL


<details>
  <summary>Click me</summary>
```sql
WITH

src_new_1 AS (

        SELECT
            N_NATIONKEY,
            ldts,
            rsrc
        FROM datavault4dbt_demo.core_Stages.stg_nation src

    ),src_new_2 AS (

        SELECT
            C_NATIONKEY,
            ldts,
            rsrc
        FROM datavault4dbt_demo.core_Stages.stg_customers src

    ),

source_new_union AS (SELECT
        N_NATIONKEY AS N_NATIONKEY,
        ldts,
        rsrc
    FROM src_new_1
    UNION ALL
    SELECT
        C_NATIONKEY AS N_NATIONKEY,
        ldts,
        rsrc
    FROM src_new_2),

earliest_ref_key_over_all_sources AS (
    SELECT
        lcte.*
    FROM source_new_union AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY N_NATIONKEY ORDER BY ldts) = 1),

records_to_insert AS (
    SELECT
        
        N_NATIONKEY,
        ldts,
        rsrc
    FROM earliest_ref_key_over_all_sources)

SELECT * FROM records_to_insert
```
</details>