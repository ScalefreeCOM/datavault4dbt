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
      high_concurrency: false
      retry_all: true
      spark_config:
        name: <session-name>   # required by the adapter
```

For a **schema-enabled** Lakehouse, `schema` must differ from the `lakehouse` name so the adapter uses three-part naming (`lakehouse.schema.table`). For a **non-schema** Lakehouse, set `schema` equal to the `lakehouse` name (two-part naming).

`reuse_session: true` together with `high_concurrency: false` keeps one Livy session for a whole `dbt build` (and across `run-operation` calls). With high-concurrency sessions each command may start a new session, which exhausts small capacities (`HTTP 430 TooManyRequestsForCapacity`).

Multiple Lakehouses in one project work through `+database` on folders (e.g. seeds in a bronze Lakehouse, raw vault in silver); the session stays bound to the profile Lakehouse and cross-Lakehouse reads, `MERGE` and `{{ this }}` in hooks resolve correctly. When `database` is set on a node, the adapter uses the custom `schema` verbatim (no `<target_schema>_<custom_schema>` prefix).

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

`RENAME COLUMN` and `DROP COLUMN` on a Delta table require column mapping (`delta.columnMapping.mode='name'`); it is not enabled by default on Fabric. The `rehash_single_*` macros set it automatically via `SET TBLPROPERTIES` (verified for hub, link, satellite, multi-active satellite and non-historized satellite).

:::warning
Enabling column mapping upgrades the Delta table protocol (reader 2 / writer 5 or higher), which may make the table unreadable through the Fabric SQL analytics endpoint / Direct Lake. Check this for tables consumed outside Spark before rehashing them.
:::

A `--full-refresh` recreates the table (`CREATE TABLE AS`) and the column mapping property is not carried over; the table is a plain Delta table again afterwards.

## YEAR-0001 TIMESTAMPS

The default `datavault4dbt.beginning_of_all_times` (`0001-01-01 00:00:01`) is stored and compared correctly as a `TIMESTAMP` in Delta on Fabric. Livy may *display* such a value as `0000-12-30T00:00:01Z` in inline query results (Julian/Gregorian rebase in result serialization); the stored value and `date_format()` / `CAST(... AS STRING)` return `0001-01-01 00:00:01`. No change of the ghost-record defaults is needed.
