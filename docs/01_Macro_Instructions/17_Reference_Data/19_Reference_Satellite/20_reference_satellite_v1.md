---
sidebar_position: 20
sidebar_label: Reference Satellite v1
title: Reference Satellite v1
---

# REFERENCE SATELLITE V1

---

This macro calculates a virtualized load end date on top of a version 0 reference satellite. This column is generated for usage in the reference tables, and only virtualized to follow the insert-only approach. Usually one version 1 sat would be created for each version 0 sat. A version 1 satellite should be materialized as a view by default.

Compared to a regular version 1 Satellite, a version 1 reference Satellite includes the parent reference keys, instead of a parents Hashkey. Besides that, it shares the same features as a standard v1 Satellite, which are:

- Calculating a virtualized load-end-date on top of a version 0 satellite
- Enforces insert-only approach without losing time ranges for business vault entities

### REQUIRED PARAMETERS

| Parameters  | Data Type    | Required  | Default Value | Explanation |
|-------------|-------------|-----------|---------------|-------------|
| ref_sat_v0  | string      | mandatory | –             | Name of the underlying version 0 reference_satellite. |
| ref_keys    | string/list | mandatory | –             | Name of the reference key(s) inside the parent reference Hub. Should be the same as defined in the underlying version 0 reference Satellite. |
| hashdiff    | string      | mandatory | –             | Name of the hashdiff column inside the underlying version 0 reference satellite. Needs to be similar to the `src_hashdiff` parameter inside the sat_v0 model. |

### OPTIONAL PARAMETERS

| Parameters          | Data Type | Required | Default Value              | Explanation |
|---------------------|-----------|----------|----------------------------|-------------|
| src_ldts            | string    | optional | datavault4dbt.ldts_alias   | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc            | string    | optional | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| ledts_alias         | string    | optional | datavault4dbt.ledts_alias  | Desired alias for the load end date column. |
| add_is_current_flag | boolean   | optional | False                      | Optional parameter to add a new column to the v1 sat based on the load end date timestamp (ledts). If set to true it will add this `is_current` flag to the v1 sat. For each record this column will be set to true if the load end date timestamp is equal to the variable end of all times. If it is not, then the record is not current therefore it will be set to false. |

## EXAMPLE 1

```jinja
{{ config(materialized='view',
        schema='Core') }}

{%- set yaml_metadata -%}
ref_sat_v0: nation_0_rs
ref_keys: N_NATIONKEY
hashdiff: hd_nation_rs
add_is_current_flag: true
{%- endset -%}      

{{ datavault4dbt.ref_sat_v1(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a reference Satellite in version 1 is created. Due to the fact that a version 1 Satellite always calculates the LoadEndDate, the two parameters ledts_alias and add_is_current_flag are set in in this macro. For detailed information on the attributes, look at the table above.

- **ref_sat_v0**:
  - __nation_0_rs__: This satellite would be the version 1 reference satellite of the underlying version 0 reference satellite for nation.
- **ref_keys**:
  - __N_NATIONKEY__: The satellite would be attached to the reference hub `nation_rh`, which has the column `N_NATIONKEY` as a reference key column.
- **hashdiff**:
  - __hd_nation_rs__: Since we recommend naming the hashdiff column similar to the name of the satellite entity, just with a prefix, this would be the hashdiff column of the reference satellite for nation.
- **ledts_alias**:
  - __loadenddate__: The `ledts` column will be called `loadenddate`
- **add_is_current_flag**:
  - __true__: This will add a new column to the v1 sat based on the load end date timestamp (ledts). For each record this column will be set to true if the load end date time stamp is equal to the variable end of all times

### COMPILED SQL

<details>
  <summary>Click me</summary>
```sql
WITH


end_dated_source AS (

    SELECT
        
        N_NATIONKEY,
        
        hd_nation_rs,
        rsrc,
        ldts,
        COALESCE(LEAD(ldts - INTERVAL '1 MICROSECOND') OVER (PARTITION BY N_NATIONKEY ORDER BY ldts),TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')) as ledts,
        
        N_COMMENT,
        N_NAME,
        N_REGIONKEY
    FROM datavault4dbt_demo.core_Core.nation_rs

)

SELECT
    
    N_NATIONKEY,
    
    hd_nation_rs,
    rsrc,
    ldts,
    ledts,
        CASE WHEN ledts = TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')
        THEN TRUE
        ELSE FALSE
        END AS IS_CURRENT,
    
        N_COMMENT,
        N_NAME,
        N_REGIONKEY
FROM end_dated_source
```
</details>
