---
sidebar_position: 40
sidebar_label: Trino
title: Trino
---

# TRINO

---

## OVERVIEW OF TRINO SPECIFIC FUNCTIONALITY

The `datavault4dbt` package relies heavily on platform-specific adaptations for tasks like hashing, string manipulation, timestamp handling, and staging. While similar to Postgres, Trino operates as a distributed SQL query engine, imposing stricter rules on data types and function compatibility.

### KEY DIFFERENCES BETWEEN TRINO AND OTHERS:

- **Datatypes:** Trino does not support the `STRING` datatype; it strictly uses `VARCHAR`.
- **Null Handling:** Trino uses standard SQL `COALESCE` rather than the `IFNULL` function often supported in other engines.
- **Hashing and Encoding:** Trino's hashing functions (like `MD5`) strictly require `VARBINARY` inputs and return `VARBINARY` outputs. Therefore:
  - We must explicitly cast strings using `TO_UTF8(VARCHAR)` before hashing.
  - We must wrap the result in `LOWER(TO_HEX(VARBINARY))` to convert the hash back into a lowercase `VARCHAR` representation. `TO_HEX` alone returns uppercase hex — `LOWER()` is required so that hash values match those produced by other adapters (BigQuery, Postgres, etc.).
- **Intervals:** Trino strictly requires intervals to be quoted logically, e.g., `INTERVAL '1' MICROSECOND` instead of `INTERVAL '00:00:00.000001'`.
- **String Concatenation constraints:** Trino's `CONCAT()` function requires at least two arguments. If a hashkey contains only one column, passing a single argument to `CONCAT()` results in a compilation error. We bypassed this by checking the column length and appending an empty string (`''`) when the length is exactly 1.

---

## ISSUES, CHALLENGES AND RESOLUTIONS

### THE GLOBAL JINJA MACRO CHALLENGE

The core `hash_standardization.sql` macro in `datavault4dbt` is a massive file containing nested Jinja `{% if %}` statements handling string standardization, concatenation, null replacement, and casting for multiple adapters (Snowflake, BigQuery, Postgres, etc.).

When initially attempting to inject Trino-specific logic into this shared file via automated text replacements, cascading compilation errors (`unexpected control flow end tag`, `mismatched input 'AS'`) were encountered. 

The sheer complexity of the concatenated strings led to mismatched parentheses and truncated Jinja tags (`-%}` becoming `-%`), completely breaking the compilation of the `datavault4dbt` package.

### TRINO SPECIFIC FUNCTION AND TYPE MATCHING

Even when the Jinja syntax was correct, the generated SQL failed native Trino compilation due to strict typing and function signatures:

1. **The `Unknown type: STRING` Error:** Errors occurred across multiple macros (staging, multi-active satellites) because the fallback logic defaulted to `CAST(... AS STRING)`. Trino does not support `STRING`, forcing a complete rewrite of the default string dtype mapping to `VARCHAR`.
2. **The `mismatched input 'AS'` Error:** When replacing `IFNULL` with `COALESCE` to support Trino, the parenthesis matching for the entire `COALESCE(TO_HEX(MD5(TO_UTF8(NULLIF(CAST(...))))))` chain broke. Trino threw vague alias parsing errors (`mismatched input 'AS'`) simply because it received a stranded `)` before the `, CAST()` fallback parameter. 
3. **The `Expected: md5(varbinary)` Error:** Trino rejected standard `MD5('string')` hashing attempts because it strictly demands `VARBINARY`. Explicit wrapping of the standardisation logic in `TO_UTF8()` before hashing was required.
4. **The `There must be two or more concatenation arguments` Error:** When hashing single-column business keys, Trino rejected `CONCAT(customer_id)`. This was resolved in `trino__hash` by dynamically checking the column length and appending an empty string: `CONCAT(customer_id, '')`.
5. **Multi-Active Array Aggregation:** Trino does not support the `STRING_AGG` function. We implemented `ARRAY_JOIN(ARRAY_AGG(... ORDER BY multi_active_key), ',')` in `trino__multi_active_concattenated_standardise` to correctly order and concatenate multi-active keys natively. The `ORDER BY` must be placed inside `ARRAY_AGG` (not via an outer `ARRAY_SORT` wrapper) and the separator must be `','` to match the implicit separator used by BigQuery's `STRING_AGG`.
6. **Uppercase hex output:** `TO_HEX()` in Trino returns uppercase hexadecimal. All hash output is wrapped in `LOWER()` so that MD5/SHA hash values are lowercase and match the output of other adapters.
7. **Quote character in attribute standardisation:** `trino__attribute_standardise` requires `'"'` (a single double-quote character) as the enclosing literal. Using `'\"'` (backslash-escaped) produces a two-character string in the Jinja/SQL context, resulting in incorrect hash input.

### RESOLUTION: SAFE INTEGRATION INTO GENERIC FILES

To break the cycle of syntax errors and adhere to `dbt` best practices, the Trino implementations were temporarily isolated by creating a dedicated `macros/supporting/trino/` directory. This allowed debugging of the `COALESCE(TO_HEX(MD5...` outputs without risking syntax errors in the generic macros.

Once perfectly tested and validated through `dbt run`, the new Trino macros (`trino__hash`, `trino__attribute_standardise`, etc.) were securely appended directly to the end of the globally shared files (`hash.sql`, `hash_standardization.sql`, etc.) and the temporary `trino/` directory was removed. This ensures the Trino logic behaves natively in `datavault4dbt` without requiring isolated folder maintenance.

---

## REHASH OPERATIONS — TRINO MEMORY CONNECTOR CONSTRAINTS

The rehash macros allow recomputing all hash keys and hashdiffs across an existing Data Vault when switching to a new hashing algorithm. The default implementation uses SQL `UPDATE` statements to modify columns in-place. The Trino **memory connector** does not support any DML row modification — `UPDATE`, `DELETE`, and `ALTER TABLE DROP COLUMN` all raise `NOT_SUPPORTED`.

### SOLUTION: CTAS WORKAROUND

Every UPDATE pattern was replaced with a three-step sequence that the Trino memory connector fully supports:

1. `CREATE TABLE {temp} AS SELECT [existing columns + computed new hashes] FROM {original}`
2. `DROP TABLE {original}`
3. `ALTER TABLE {temp} RENAME TO {original_name}`

### FIVE SPECIFIC ISSUES AND THEIR FIXES

**1. UPDATE Not Supported**
`NOT_SUPPORTED: This connector does not support modifying table rows`

All five `*_update_statement` macros used `UPDATE SET` to write new hash values. All five `rehash_single_*.sql` files under `macros/rehashing/single_entities/trino/` were rewritten using the CTAS workaround.

**2. Stale `_new` Columns After Partial Failures**
`DUPLICATE_COLUMN_NAME: 'hk_xxx_new'`

After a failed partial run, a prior `ALTER TABLE ADD COLUMN ... hk_xxx_new` step could leave a stale column. The CTAS `SELECT *` would include it, and adding the freshly computed column under the same name caused a duplicate. Fix: use `adapter.get_columns_in_relation()` and filter columns ending in `_new` before building the CTAS `SELECT` list.

**3. Link Rehash — Unresolvable Hub Alias Columns**
`COLUMN_NOT_FOUND: Column 'hub1.o_orderkey' cannot be resolved`

The original link update macro referenced aliased columns (`hub1.business_key`) inside an `UPDATE` with no `FROM` clause. This was structurally broken independently of Trino. Fix: the CTAS mirrors the Postgres implementation by explicitly `LEFT JOIN`ing each hub table so that business keys are in scope when computing the new link hashkey.

**4. Duplicate `_deprecated` Column on Re-run**
`COLUMN_ALREADY_EXISTS: 'hk_xxx_deprecated'`

If a run failed after the first rename (`hk → hk_deprecated`) but before the second (`hk_new → hk`), the table already had a `_deprecated` column. The `clean_cols` filter (which only excluded `_new`) left `_deprecated` in the CTAS select, then the next rename step tried to create it again. Fix: filter both `_new` **and** `_deprecated` suffixes from `clean_cols`.

**5. DROP COLUMN Not Supported**
`NOT_SUPPORTED: This connector does not support dropping columns`

The final cleanup step in `custom_alter_relation_add_remove_columns` called `ALTER TABLE DROP COLUMN`. Fix: the `remove_columns` path in `trino__custom_alter_relation_add_remove_columns` was rewritten with the same CTAS + DROP + RENAME pattern, keeping only the columns not in the drop list.

### FILES CHANGED FOR REHASH SUPPORT

| File | Change |
|---|---|
| `macros/rehashing/single_entities/trino/rehash_single_hub.sql` | Full CTAS rewrite |
| `macros/rehashing/single_entities/trino/rehash_single_satellite.sql` | Full CTAS rewrite |
| `macros/rehashing/single_entities/trino/rehash_single_link.sql` | CTAS rewrite + explicit hub JOINs |
| `macros/rehashing/single_entities/trino/rehash_single_ma_satellite.sql` | CTAS rewrite with GROUP BY subquery |
| `macros/rehashing/single_entities/trino/rehash_single_nh_satellite.sql` | Full CTAS rewrite |
| `macros/rehashing/internal_overwrites/alter_table_add_col.sql` | `remove_columns` path CTAS workaround |
| `macros/rehashing/internal_overwrites/get_rename_column_sql.sql` | New `trino__custom_get_rename_column_sql` |

> **Note:** These constraints apply to the Trino **memory connector** used in the local test environment. Disk-backed Trino connectors (e.g., Delta Lake, Iceberg, Hive) do support `UPDATE` and `DROP COLUMN` via connector-specific implementations. The CTAS workaround is safe and performant for both.