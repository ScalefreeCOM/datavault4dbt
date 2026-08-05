---
sidebar_position: 40
sidebar_label: SQL Server
title: SQL Server
---

# SQL SERVER

---

Support for Microsoft SQL Server is available from v2.0.0. The SQL Server implementation is T-SQL based and behaves similarly to the Synapse and Fabric adapters.

## HASH DATA TYPE

The SQL Server macros fall back to `VARBINARY(16)` internally, but if you copied the package defaults into your own `dbt_project.yml`, the `STRING` value will take precedence.

### RECOMMENDED CONFIGURATION

- **Switch to a binary hash type:** Set `datavault4dbt.hash_datatype` in your `dbt_project.yml` to `VARBINARY(16)` (for MD5). This ensures hash keys and hashdiffs are stored efficiently and compared correctly on SQL Server.

## HASH INPUT DATA TYPE

Before hashing, both the single input columns and the concatenated payload are casted to a string datatype. On SQL Server this defaults to `VARCHAR(MAX)`, which is a large-value type: it is stored off-row, cannot be held in memory the same way as a regular `VARCHAR(n)`, and blocks several optimizations for the `REPLACE()`, `UPPER()` and `HASHBYTES()` calls wrapped around it. On wide satellites this can dominate the runtime of a load.

Both casts are configurable per adapter through the global variables `datavault4dbt.hash_input_attribute_dtype` and `datavault4dbt.hash_input_concat_dtype`:

```yaml
vars:
  datavault4dbt.hash_input_attribute_dtype: {"sqlserver": "VARCHAR(8000)"}
  datavault4dbt.hash_input_concat_dtype: {"sqlserver": "VARCHAR(8000)"}
```

:::warning
`VARCHAR(8000)` is the largest non-large-value `VARCHAR` on SQL Server. Any hash input longer than the configured length is truncated **silently**, which changes the resulting hashkeys and hashdiffs. Two rows that only differ behind the truncation point produce the same hash. Before lowering these values, verify that the concatenated input of your widest entity stays below the limit, and treat a later change as a full reload of all affected entities.
:::

### MULTI ACTIVE SATELLITES

Multi Active Satellites are excluded from both variables and always stay on `VARCHAR(MAX)`, so you can shorten the cast for all other entities without touching them.

The reason is `STRING_AGG()`, which aggregates the payload of all active records of one group before it is hashed. `STRING_AGG()` only returns `VARCHAR(MAX)` if its input expression is `VARCHAR(MAX)`; with a shorter input it returns `VARCHAR(8000)` and **raises an error** as soon as the aggregated result of a single group exceeds 8000 bytes. Since that limit applies to a whole group instead of a single record, it is far easier to hit than the per-record limit of a regular Satellite.

