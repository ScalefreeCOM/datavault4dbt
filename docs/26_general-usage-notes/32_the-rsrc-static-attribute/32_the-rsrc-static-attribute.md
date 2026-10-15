---
sidebar_position: 32
sidebar_label: The rsrc_static Attribute
title: The rsrc_static Attribute
---

# THE RSRC_STATIC ATTRIBUTE

---

## APPLIANCE

This page explains the rsrc_static attribute, which is used in multiple macros:

- Hubs
- Links
- Non-Historized Links
- Record Tracking Satellites
- Reference Hubs

## PURPOSE

The rsrc_static attribute is only used, when an entity is loaded by multiple sources. The main purpose of the rsrc_static attribute is, to optimize performance of incremental loads.

Typically you only want to look at those records of the stage, that are newer than the maximum load date inside the target entity. This behavior especially improves performance, if the underlying staging areas hold a large amount of historical loads, since the number of rows scanned is drastically reduced.

If an entity is loaded from multiple sources, each source should be filtered down to records, that are newer than the maximum load date of this exact source, not the overall maximum load date inside the target entity. To allow that behavior, the maximum load date for each different source needs to be calculated.

Sometimes the record source column inside one staging area is not always the same, but has some dynamic parts in it. Sometimes the rsrc column includes the ldts of each load and could look something like this: `SALESFORCE/Partners/2022-01-01T07:00:00`.

Obviously the timestamp part inside that rsrc would change from load to load, and we now need to identify parts of it that will be static over all loads. In the shown example it would be `SALESFORCE/Partners/`. That expression now needs to be enriched with wildcards to catch all occurrences of the static part of the record source. Therefore the rsrc_static would be `SALESFORCE/Partners/%`.

## WILDCARDS

On all data platforms, the macros compare the record source column with rsrc_static by using the SQL `LIKE` operator. Therefore the `LIKE` syntax of your data platform applies:

- `%` matches any sequence of characters, including an empty one.
- `_` matches exactly one character.

The character `*` is not a wildcard. An rsrc_static like `*/SALESFORCE/Partners/*` is compared as a literal string and does not match the record sources. Use `%` instead.

### LITERAL UNDERSCORES AND PERCENT SIGNS

Since `_` matches any single character, `SALESFORCE/ORDER_ITEMS/%` also matches a record source like `SALESFORCE/ORDERXITEMS/2022-01-01T07:00:00`. An unescaped `_` still matches a literal underscore, so this only matters if another record source in the same stage differs from the intended one exactly at that position. The examples in the macro documentation therefore do not escape `_`.

To match only a literal `_`, escape it. The syntax depends on the data platform:

| Data platform | rsrc_static with a literal `_` |
|---|---|
| Snowflake, BigQuery, Databricks, Redshift | `'SALESFORCE/ORDER\\_ITEMS/%'` |
| PostgreSQL, Oracle, Trino, Exasol | `'SALESFORCE/ORDER\_ITEMS/%'` |
| SQL Server, Azure Synapse, Microsoft Fabric | `'SALESFORCE/ORDER[_]ITEMS/%'` |

A literal `%` is escaped the same way: replace `_` with `%` in the table, for example `'SALESFORCE/100\\%/%'` on Snowflake or `'SALESFORCE/100[%]/%'` on SQL Server.

The values in the table apply to single-quoted strings in `yaml_metadata`. In a Jinja dictionary or a double-quoted YAML string, type every backslash twice, for example `'SALESFORCE/ORDER\\\\_ITEMS/%'` on Snowflake. The PostgreSQL value assumes `standard_conforming_strings = on`, which is the default.

On the platforms of the first two rows, the backslash is the escape character. A record source that contains a literal backslash therefore needs it doubled in rsrc_static: `\\\\` in the first row, `\\` in the second row. On SQL Server, Azure Synapse and Microsoft Fabric, `[` and `]` are wildcards as well. Write a literal `[` as `[[]`.

Inside `yaml_metadata`, always quote an rsrc_static that starts with `%`. YAML does not allow an unquoted value to start with `%`.

Do not escape `_` or `%` in the rsrc_static of Record Tracking Satellites. They write the rsrc_static value itself into the record source column and look it up again with `LIKE`. On most data platforms, an escaped value does not match its own stored text, and the incremental load then loses the high-water mark of that value.

## SPECIAL CASES

### NO DYNAMIC PART

If the record source column of a staging area has no dynamic part and hence is always the same over all rows of a specific staging area, rsrc_static needs to be set to that whole static part, without wildcards.

### MULTIPLE DYNAMIC PARTS

Sometimes multiple different wildcard expressions belong to the same record source. In that case, rsrc_static can also be set to a list of wildcard expressions to catch all cases that belong together.

### NO STATIC PART

Sometimes the record source column of a stage has no static part, and the rows can not be grouped together again. In such a case rsrc_static is not to be set at all. The downside is, that the loading procedures do not benefit of the performance boost provided by the rsrc_static parameter. We recommend re-evaluating the contents of the record source column in such a case, to create at least a partially static content.
