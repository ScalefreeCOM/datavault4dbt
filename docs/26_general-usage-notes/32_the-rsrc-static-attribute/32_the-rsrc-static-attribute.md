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

## PURPOSE

The rsrc_static attribute is only used, when an entity is loaded by multiple sources. The main purpose of the rsrc_static attribute is, to optimize performance of incremental loads.

Typically you only want to look at those records of the stage, that are newer than the maximum load date inside the target entity. This behavior especially improves performance, if the underlying staging areas hold a large amount of historical loads, since the number of rows scanned is drastically reduced.

If an entity is loaded from multiple sources, each source should be filtered down to records, that are newer than the maximum load date of this exact source, not the overall maximum load date inside the target entity. To allow that behavior, the maximum load date for each different source needs to be calculated.

Sometimes the record source column inside one staging area is not always the same, but has some dynamic parts in it. Sometimes the rsrc column includes the ldts of each load and could look something like this: `SALESFORCE/Partners/2022-01-01T07:00:00`.

Obviously the timestamp part inside that rsrc would change from load to load, and we now need to identify parts of it that will be static over all loads. In the shown example it would be `SALESFORCE/Partners`. That expression now needs to be enriched with wildcard expressions to catch all occurrences of the static part of the record source. In BigQuery the wildcard would be `*` and therefore the rsrc_static would be `*/SALESFORCE/Partners/*`.

## SPECIAL CASES

### NO DYNAMIC PART

If the record source column of a staging area has no dynamic part and hence is always the same over all rows of a specific staging area, rsrc_static needs to be set to that whole static part, without wildcards.

### MULTIPLE DYNAMIC PARTS

Sometimes multiple different wildcard expressions belong to the same record source. In that case, rsrc_static can also be set to a list of wildcard expressions to catch all cases that belong together.

### NO STATIC PART

Sometimes the record source column of a stage has no static part, and the rows can not be grouped together again. In such a case rsrc_static is not to be set at all. The downside is, that the loading procedures do not benefit of the performance boost provided by the rsrc_static parameter. We recommend re-evaluating the contents of the record source column in such a case, to create at least a partially static content.

### SNOWFLAKE

Snowflake requires special handling when wildcard expressions are used in rsrc_static.

By default, rsrc_static values are compared using the `=` operator and the wildcard character is `*`. However, Snowflake does not interpret wildcard characters when using an equality comparison.

For example, the following condition does not perform pattern matching in Snowflake:

`WHERE dw_record_source = '*/SALESFORCE/Partners/*'`

Instead, Snowflake interprets `*/SALESFORCE/Partners/*` as a literal string.

To support dynamic record sources, the Snowflake implementation of the macros therefore uses the `LIKE` operator whenever rsrc_static is evaluated. Consequently, the wildcard character for Snowflake is `%` instead of `*`.

For example, the equivalent Snowflake rsrc_static value would be:

`%/SALESFORCE/Partners/%`

This results in a comparison such as:

`WHERE dw_record_source LIKE '%/SALESFORCE/Partners/%'`

Therefore, when configuring rsrc_static for Snowflake:

- Use `%` as the wildcard character instead of `*`.
- Wildcard-based rsrc_static values are evaluated using the `LIKE` operator.
- Static record sources without dynamic parts can still be configured without wildcard characters.
