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

Both casts are configurable per adapter. **Shorten only the concatenated payload cast:**

```yaml
vars:
  datavault4dbt.hash_input_concat_dtype: {"sqlserver": "VARCHAR(8000)"}
```

`VARCHAR(8000)` is the largest non-large-value `VARCHAR` on SQL Server, so this takes the value that `HASHBYTES()` consumes out of large-value handling while leaving the individual column casts unbounded.

:::warning
Any hash input longer than the configured length is truncated **silently**, which changes the resulting hashkeys and hashdiffs. Two rows that only differ behind the truncation point produce the same hash, and a Satellite then stops recording changes confined to that region. Before lowering this value, check that the concatenated input of your widest entity stays below the limit:

- sum the lengths of all columns in the hash
- add 2 characters per column for the quoting the package applies
- add the length of `concat_string` between each pair of columns

Treat a later change to this value as a full reload of all affected entities.
:::

### THE PER-COLUMN CAST

`datavault4dbt.hash_input_attribute_dtype` sets the datatype each column is cast to **before** the columns are joined. Leave it at its default of `VARCHAR(MAX)` on SQL Server.

The reason is the join itself. The package joins the columns with `CONCAT()` / `CONCAT_WS()`, and those functions derive their own maximum length from their arguments: the result is capped at 8000 characters unless at least one argument is an unbounded type. Bounding every column therefore bounds the **whole** concatenated payload, and it does so before the concatenated-payload cast runs, so that cast cannot widen it back:

```sql
DECLARE @a VARCHAR(8000) = REPLICATE('a', 8000);
DECLARE @b VARCHAR(8000) = REPLICATE('b', 8000);

SELECT LEN(CONCAT_WS('||', @a, @b))                        AS both_bounded,      -- 8000
       LEN(CAST(CONCAT_WS('||', @a, @b) AS VARCHAR(MAX)))  AS bounded_then_cast, -- 8000
       LEN(CONCAT_WS('||', CAST(@a AS VARCHAR(MAX)), @b))  AS one_unbounded;     -- 16002
```

Consequence: with `VARCHAR(8000)` per column, an entity of 20 columns averaging 500 characters concatenates to about 10 078 characters, the last ~2 000 are dropped, and roughly the final four columns never reach the hash function. The hashdiff stops depending on them, so a change confined to those columns produces no new hashdiff and the Satellite records nothing. No error is raised.

If you still need the per-column cast, the safe bound is approximately `8000 / number_of_columns_in_the_hash`, and it must be re-checked whenever a column is added to the entity.

### MULTI ACTIVE SATELLITES

`hash_input_concat_dtype` does not reach Multi Active Satellites: `multi_active_concattenated_standardise` keeps its hardcoded datatype, so their aggregated payload can never overflow `STRING_AGG()`.

`STRING_AGG()` is the reason. It only returns `VARCHAR(MAX)` if its input expression is `VARCHAR(MAX)`; with a shorter input it returns `VARCHAR(8000)` and **raises an error** as soon as the aggregated result of a single group exceeds 8000 bytes. Since that limit applies to a whole group instead of a single record, it is far easier to hit than the per-record limit of a regular Satellite.

`hash_input_attribute_dtype` **does** reach them, because the per-column cast is shared with all other entities. A Multi Active Satellite therefore needs the same width check as a regular Satellite.

