---
sidebar_position: 39

sidebar_label: MS Fabric
title: MS Fabric
---

# MS FABRIC

---

## LIMITATION OF AMOUNT FOR COLUMNS IN HASHDIFF

Due to the max amount of arguments of the `CONCAT_WS()`-function the current (v1.9.11) maximum amount of columns for a singular hashdiff on MS Fabric is 253.

## DATE AND TIME COLUMNS IN HASH INPUTS

:::warning
On MS Fabric, every hash input attribute is standardized via `CONVERT(<hash_input_attribute_dtype>, <column>, 1)`. Style `1` is required to get a hex representation of `VARBINARY` columns, but for date and time datatypes it means `mm/dd/yy`. As a result, `DATETIME2`, `DATETIME`, `SMALLDATETIME` and `DATETIMEOFFSET` columns lose their time portion, and `DATE` columns are reduced to a two-digit year. Two rows that only differ in the time (or century) of such a column produce the **same** hashkey or hashdiff.

To avoid this, explicitly convert all date and time columns that are part of a hashkey or hashdiff into a string with a lossless format inside `derived_columns`, for example using style `121` (`yyyy-mm-dd hh:mi:ss.mmm`), and use the derived column as hash input instead:

```yaml
derived_columns:
  order_date_hashinput:
    value: "CONVERT(VARCHAR(4000), order_date, 121)"
    datatype: "VARCHAR(4000)"
hashed_columns:
  hk_example_l:
    - customer_id
    - order_date_hashinput
```

Changing the hash input of existing entities changes the resulting hash values and therefore requires a full reload of the affected entities.
:::

## ADAPTER SPECIFIC VARIABLE
The variable `datavault4dbt.set_casing` can be used to force all column names to be uppercased or lowercased. Allowed values for the variable are `upper`, `uppercase`, `lower`, `lowercase`.