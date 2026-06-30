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

