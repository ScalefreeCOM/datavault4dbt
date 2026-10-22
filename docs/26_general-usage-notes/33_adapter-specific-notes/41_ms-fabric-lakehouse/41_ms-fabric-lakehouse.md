---
sidebar_position: 41
sidebar_label: MS Fabric Lakehouse
title: MS Fabric Lakehouse
---

# MS FABRIC LAKEHOUSE

---

This article covers Microsoft Fabric Lakehouse via the [dbt-fabricspark](https://github.com/microsoft/dbt-fabricspark) adapter (`type: fabricspark`, Spark SQL dialect over the Livy API). The implementation derives from the Databricks macros.

## PROFILE NOTES

Example target for a schema-enabled Lakehouse with Azure CLI authentication (`pip install dbt-fabricspark[cli]`, `az login` required):

```yaml
dv4dbt_fabricspark:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: livy
      authentication: CLI
      endpoint: https://api.fabric.microsoft.com/v1
      workspaceid: <your-workspace-id>
      lakehouseid: <your-lakehouse-id>
      lakehouse: <lakehouse-name>
      schema: <schema-name>
      threads: 1
      reuse_session: true
      retry_all: true
      spark_config:
        name: <session-name>   # required by the adapter
```

For a **schema-enabled** Lakehouse, `schema` must differ from the `lakehouse` name so the adapter uses three-part naming (`lakehouse.schema.table`). For a **non-schema** Lakehouse, set `schema` equal to the `lakehouse` name (two-part naming).

## QUALIFY NOT AVAILABLE

Fabric Spark rejects the `QUALIFY` clause. Deduplication in the `fabricspark__` table macros therefore uses the `ROW_NUMBER() OVER (...) AS rn` + `WHERE rn = 1` CTE pattern (same shape as the PostgreSQL implementation).

## COLUMN ESCAPING

Spark treats double quotes as string literals when ANSI mode is off. Set the global variables:

```yaml
vars:
  escape_char_left: '`'
  escape_char_right: '`'
```

Identifier resolution is case-insensitive by default; avoid mixed-case column names.

## REHASHING REQUIRES DELTA COLUMN MAPPING

`RENAME COLUMN` and `DROP COLUMN` on a Delta table require column mapping (`delta.columnMapping.mode='name'`). The `rehash_single_*` macros set it automatically via `SET TBLPROPERTIES`.

:::warning
Enabling column mapping upgrades the table protocol and makes the table unreadable through the Fabric SQL analytics endpoint / Direct Lake.
:::

## KNOWN LIMITATION: YEAR-0001 TIMESTAMPS

`TIMESTAMP` values in year 0001 (the default `datavault4dbt.beginning_of_all_times`) are shifted by Spark's Julian/Gregorian calendar rebase (e.g. `0001-01-01` may display as `0000-12-30`). This is under validation; `to_date`/`DATE` values round-trip correctly.
