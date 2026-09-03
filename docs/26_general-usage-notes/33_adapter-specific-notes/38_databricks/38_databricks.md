---
sidebar_position: 38
sidebar_label: Databricks
title: Databricks
---

# DATABRICKS

---

This article focuses on Databricks-specific considerations for users of the datavault4dbt package.

## HASH DATATYPE

`datavault4dbt.hash_datatype` accepts a string type (`STRING`, the default, or a `VARCHAR`/`CHAR`/`TEXT` spelling) or `BINARY`; any other value raises a compiler error. Databricks `BINARY` takes no length modifier — use `'BINARY'`, not `'BINARY(16)'`.

Databricks' `md5()`, `sha1()` and `sha2()` return hex *text*, not the digest. `STRING` stores that text (32 characters for MD5); `BINARY` decodes it with `UNHEX()` and stores the digest (16 bytes for MD5) — half the width, and byte-identical to Snowflake, SQL Server and BigQuery.

Databricks clients usually render `BINARY` as base64, so use `lower(hex(<hashkey>))` to read a hash key or compare it against another platform.

:::caution Breaking change — binary hash values differ
Up to and including **2.1.0** a binary `hash_datatype` emitted `CAST(md5(…) AS BINARY)`, storing the UTF-8 bytes of the hex text rather than the digest — double the intended width, and different bytes from every other adapter.

Databricks projects on a binary `hash_datatype` must fully refresh or [rehash](../../41_rehashing/41_rehashing.md) the Raw Vault. Projects on `STRING` are unaffected.
:::

## MULTI-ACTIVE HASHDIFF

From **v2.0.0**, the multi-active hashdiff on Databricks uses the native `LISTAGG` function with an explicit `WITHIN GROUP (ORDER BY ...)` clause. This produces a deterministic, correctly-ordered aggregation without requiring a derived `ROW_NUMBER` workaround.

```sql
LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY multi_active_key)
```

### MINIMUM RUNTIME REQUIREMENT

`LISTAGG ... WITHIN GROUP (ORDER BY ...)` requires **Databricks Runtime 16.4 LTS or later** (or an equivalent Databricks SQL warehouse version). If you are on an older runtime, upgrade before using v2.0.0 multi-active satellites, or the model will fail to compile with a function-not-found error.


## ADAPTER SPECIFIC VARIABLE

The variable `datavault4dbt.set_casing` can be used to force all column names to be uppercased or lowercased. Allowed values for the variable are `upper`, `uppercase`, `lower`, `lowercase`.