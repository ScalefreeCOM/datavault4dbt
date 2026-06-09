---
sidebar_position: 38
sidebar_label: Databricks
title: Databricks
---

# DATABRICKS

---

This article focuses on Databricks-specific considerations for users of the datavault4dbt package.

## MULTI-ACTIVE HASHDIFF

From **v2.0.0**, the multi-active hashdiff on Databricks uses the native `LISTAGG` function with an explicit `WITHIN GROUP (ORDER BY ...)` clause. This produces a deterministic, correctly-ordered aggregation without requiring a derived `ROW_NUMBER` workaround.

```sql
LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY multi_active_key)
```

### MINIMUM RUNTIME REQUIREMENT

`LISTAGG ... WITHIN GROUP (ORDER BY ...)` requires **Databricks Runtime 10.4 LTS or later** (or an equivalent Databricks SQL warehouse version). If you are on an older runtime, upgrade before using v2.0.0 multi-active satellites, or the model will fail to compile with a function-not-found error.


## ADAPTER SPECIFIC VARIABLE

The variable `datavault4dbt.set_casing` can be used to force all column names to be uppercased or lowercased. Allowed values for the variable are `upper`, `uppercase`, `lower`, `lowercase`.