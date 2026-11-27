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

On MS Fabric, every hash input attribute is standardized via `CONVERT(<hash_input_attribute_dtype>, <column>, 121)`. Style `121` keeps the full precision of date and time datatypes, e.g. `DATETIME2` becomes `yyyy-mm-dd hh:mi:ss.fffffff`.

Previously style `1` was used, which turned date and time datatypes into `mm/dd/yy`. `DATETIME2`, `DATETIME`, `SMALLDATETIME` and `DATETIMEOFFSET` columns lost their time portion, and `DATE` columns were reduced to a two-digit year, so two rows that only differed in the time (or century) of such a column produced the **same** hashkey or hashdiff.

:::warning
Switching from style `1` to style `121` changes all hashkeys and hashdiffs that have a date or time column as input. Changing the hash input of existing entities requires a full reload or a rehash of the affected entities. If you already convert date and time columns with a derived column (`CONVERT(VARCHAR(4000), <column>, 121)`), those hash values stay unchanged.
:::

## BINARY COLUMNS IN HASH INPUTS

:::warning
`VARBINARY` / `BINARY` columns return `NULL` with style `121`, so they would silently be hashed as the null placeholder. If a hashkey or hashdiff needs a binary column as input, convert it into its hex string representation inside `derived_columns` and use the derived column as hash input instead:

```yaml
derived_columns:
  document_checksum_hashinput:
    value: "CONVERT(VARCHAR(4000), document_checksum, 1)"
    datatype: "VARCHAR(4000)"
hashed_columns:
  hd_document_s:
    is_hashdiff: true
    columns:
      - document_checksum_hashinput
      - document_name
```

Style `1` returns the same `0x...` string that was hashed before, so hash values over binary columns stay unchanged.
:::

## FLOAT AND REAL COLUMNS IN HASH INPUTS

:::note
Style `121` is not a float style, so `FLOAT` and `REAL` columns are converted with only 6 significant digits, e.g. `1234.56789012` becomes `1234.57`, and large values switch to scientific notation (`1.23457e+006`). Two float values that only differ after the 6th digit produce the **same** hashkey or hashdiff, and the hash values differ from other adapters, which keep all digits.

If the exact value of a float column matters, convert it inside `derived_columns` before hashing, e.g. with the lossless style `3`:

```yaml
derived_columns:
  measurement_value_hashinput:
    value: "CONVERT(VARCHAR(4000), measurement_value, 3)"
    datatype: "VARCHAR(4000)"
```

Style `3` always uses scientific notation with 17 digits (`1.2345678901199999e+003`). Every distinct value gets a distinct hash input, but the hash values still differ from other adapters.
:::

## ADAPTER SPECIFIC VARIABLE
The variable `datavault4dbt.set_casing` can be used to force all column names to be uppercased or lowercased. Allowed values for the variable are `upper`, `uppercase`, `lower`, `lowercase`.