---
sidebar_position: 40
sidebar_label: Trino
title: Trino
---

# TRINO

---

## Key Differences from Other Adapters

When using `datavault4dbt` with Trino, the following platform-specific behaviours apply:

- **Datatypes:** Trino does not support the `STRING` datatype; use `VARCHAR` instead. The package's `datavault4dbt.string_default_dtype` variable handles this automatically.
- **Hashing:** Trino's `MD5()` function requires `VARBINARY` input and returns uppercase hex. The package wraps hash inputs in `TO_UTF8()` and the result in `LOWER(TO_HEX())` to produce lowercase hex strings consistent with other adapters.
- **String Aggregation:** Trino does not support `STRING_AGG`. For multi-active satellite hashdiff calculations, the package uses `ARRAY_JOIN(ARRAY_AGG(... ORDER BY multi_active_key), ',')` instead.
- **CONCAT constraints:** Trino's `CONCAT()` requires at least two arguments. For single-column business keys, the package appends an empty string to satisfy this requirement.

## Rehash Operations — Memory Connector Constraints

The Trino **memory connector** (typically used for local development and testing) does not support `UPDATE`, `DELETE`, or `ALTER TABLE DROP COLUMN`. The datavault4dbt rehash macros address this with a `CREATE TABLE AS SELECT` + `DROP TABLE` + `ALTER TABLE RENAME TO` pattern instead.

> This applies to the memory connector only. Disk-backed connectors (Hive, Iceberg, Delta Lake) support these operations natively.
