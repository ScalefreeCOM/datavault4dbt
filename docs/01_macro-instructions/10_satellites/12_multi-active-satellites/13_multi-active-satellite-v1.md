---
sidebar_position: 13
sidebar_label: Multi-Active Satellite v1
title: Multi-Active Satellite v1
---

# MULTI-ACTIVE SATELLITE V1

---

This macro calculates the load end dates for multi active data, based on a multi active attribute. It must be based on a version 0 multi-active satellite, that would then hold multiple records per hashkey+ldts combination.

Features:

- Calculates virtualized load-end-dates to correctly identify multiple active records per batch
- Enforces insert-only approach by view materialization
- Allows multiple attributes to be used as the multi-active-attribute

### REQUIRED PARAMETERS

| Parameters   | Data Type                | Required  | Default Value | Explanation |
|--------------|--------------------------|-----------|---------------|-------------|
| sat_v0       | string                   | mandatory | –             | Name of the underlying version 0 multi-active satellite. |
| hashkey      | string                   | mandatory | –             | Name of the parent hashkey column inside the version 0 satellite. Would either be the hashkey of a hub or a link. Needs to be similar to the `parent_hashkey` parameter inside the sat_v0 model. |
| hashdiff     | string                   | mandatory | –             | Name of the hashdiff column inside the underlying version 0 satellite. Needs to be similar to the `src_hashdiff` parameter inside the sat_v0 model. Must include the ma_attribute in calculation. |
| ma_attribute | string \| list of strings | mandatory | –             | Name of the multi active attribute inside the v0 satellite. This needs to be identified under the requirement that the combination of hashkey + ldts + ma_attribute is unique over the entire stage / satellite. |

### OPTIONAL PARAMETERS

| Parameters          | Data Type | Required | Default Value              | Explanation |
|---------------------|-----------|----------|----------------------------|-------------|
| src_ldts            | string    | optional | datavault4dbt.ldts_alias   | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc            | string    | optional | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source models. Is optional, will use the global variable `datavault4dbt.rsrc_alias`. Needs to use the same column name as defined as alias inside the staging model. |
| ledts_alias         | string    | optional | datavault4dbt.ledts_alias  | Desired alias for the load end date column. |
| add_is_current_flag | boolean   | optional | False                      | Optional parameter to add a new column to the v1 sat based on the load end date timestamp (ledts). If set to true it will add this `is_current` flag to the v1 sat. For each record this column will be set to true if the load end date timestamp is equal to the variable end of all times. If it is not, then the record is not current and therefore it will be set to false. |

## EXAMPLE 1

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
sat_v0: 'contact_phonenumer_v0_mas'
hashkey: 'hk_contact_h'
hashdiff: 'hd_contact_phonenumber_mas' 
ma_attribute:
    - phone_type
    - iid
ledts_alias: 'valid_to'
add_is_current_flag: true
{%- endset -%}    

{{ datavault4dbt.ma_sat_v1(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

It is important that for the underlying sat v0, the regular sat_v0 macro is used.

- **sat_v0**:
  - __contact_phonenumer_v0_mas__: This satellite would be the version 1 satellite of the underlying version 0 data satellite for contact phonenumber.
- **hashkey**:
  - __hk_contact_h__: The satellite would be attached to the hub contact, which has the column `hk_contact_h` as a hashkey column.
- **hashdiff**:
  - __hd_contact_phonenumber_mas__: Since we recommend naming the hashdiff column similar to the name of the satellite entity, just with a prefix, this would be the hashdiff column of the data satellite for contacts.
- **ma_attribute**:
  - __[`phone_type`, `iid`]__: If a contact could have multiple mobile phonenumbers, the phone_type alone would not be enough to uniquely identify a record inside a hashkey+ldts combination. Additionally the attribute iid, which is an increasing identifier within a phone_type, is added as a ma_attribute.
- **ledts_alias**:
  - __valid_to__: The `ledts` column will be called `valid_to`.
- **add_is_current_flag**:
  - __true__: This will add a new column to the v1 sat based on the load end date timestamp (ledts). For each record this column will be set to true if the load end date time stamp is equal to the variable end of all times