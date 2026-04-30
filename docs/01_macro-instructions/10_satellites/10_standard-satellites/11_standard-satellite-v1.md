---
sidebar_position: 11
sidebar_label: Standard Satellite v1
title: Standard Satellite v1
---

# STANDARD SATELLITE V1

---

This macro calculates a virtualized load end date on top of a version 0 satellite. This column is generated for usage in the PIT tables, and only virtualized to follow the insert-only approach. Usually one version 1 sat would be created for each version 0 sat. A version 1 satellite should be materialized as a view by default.

Features:

- Calculating a virtualized load-end-date on top of a version 0 satellite
- Enforces insert-only approach without losing time ranges for business vault entities

### REQUIRED PARAMETERS

| Parameters | Data Type | Required  | Default Value | Explanation |
|------------|-----------|-----------|---------------|-------------|
| sat_v0     | string    | mandatory | –             | Name of the underlying version 0 satellite. |
| hashkey    | string    | mandatory | –             | Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a hub or a link. Needs to be similar to the `parent_hashkey` parameter inside the sat_v0 model. |
| hashdiff   | string    | mandatory | –             | Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the `src_hashdiff` parameter inside the sat_v0 model. |

### OPTIONAL PARAMETERS

| Parameters          | Data Type | Required | Default Value              | Explanation |
|---------------------|-----------|----------|----------------------------|-------------|
| src_ldts            | string    | optional | datavault4dbt.ldts_alias   | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc            | string    | optional | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| ledts_alias         | string    | optional | datavault4dbt.ledts_alias  | Desired alias for the load end date column. |
| add_is_current_flag | boolean   | optional | False                      | Optional parameter to add a new column to the v1 sat based on the load end date timestamp (ledts). If set to true it will add this `is_current` flag to the v1 sat. For each record this column will be set to true if the load end date timestamp is equal to the variable end of all times. If it is not, then the record is not current therefore it will be set to false. |
| include_payload     | boolean   | optional | True                       | Optional parameter to specify if the v1 sat should have the payload columns from sat v0 or not. |

## EXAMPLE 1

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
sat_v0: 'account_v0_s'
hashkey: 'hk_account_h'
hashdiff: 'hd_account_s'   
ledts_alias: 'loadenddate'
add_is_current_flag: true
{%- endset -%}    

{{ datavault4dbt.sat_v1(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a regular standard Satellite in version 1 is created. Due to the fact, that a version 1 Satellite always calculates the LoadEndDate, the two parameters ledts_alias and add_is_current_flag are set in in this macro. For detailed information on the attributes, look at the table above.

- **sat_v0**:
  - __account_v0_s__: This satellite would be the version 1 satellite of the underlying version 0 data satellite for account.
- **hashkey**:
  - __hk_account_h__: The satellite would be attached to the hub account, which has the column `hk_account_h` as a hashkey column.
- **hashdiff**:
  - __hd_account_s__: Since we recommend naming the hashdiff column similar to the name of the satellite entity, just with a prefix, this would be the hashdiff column of the data satellite for account.
- **ledts_alias**:
  - __loadenddate__: The `ledts` column will be called `loadenddate`
- **add_is_current_flag**:
  - __true__: This will add a new column to the v1 sat based on the load end date timestamp (ledts). For each record this column will be set to true if the load end date time stamp is equal to the variable end of all times