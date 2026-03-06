---
sidebar_position: 24
sidebar_label: Snapshot Control v0
title: Snapshot Control v0
---

# SNAPSHOT CONTROL V0

---

This macro creates a snapshot table to control snapshot-based tables like PITs and Bridges. The snapshot table will hold daily snapshots starting at a specific start_date and has a configurable daytime. Usual application would involve creating one snapshot table per Data Vault environment, and therefore creating one dbt model using this macro. The model needs to be scheduled daily, with a execution time that matches your desired snapshot time, since the macro automatically inserts all snapshots until the current day, no matter which time it is, and a snapshot for 08:00:00 should be calculated at that time. So if the snapshot table is configured to have a ‘daily_snapshot_time’ of ’07:00:00′, all snapshots in the table will have the timestamp ’07:00:00′. Therefore you need to schedule the building of the snapshot table also to ’07:00:00′.

In addition to the actual snapshot-datetimestamp (sdts), the macro generates the following columns:

| Column           | Data Type | Explanation |
|------------------|-----------|-------------|
| replacement_sdts | timestamp | Allows users to replace a sdts with another one, without having to update the actual sdts column. By default this column is filled with the regular sdts. |
| caption          | string    | Allows users to title their snapshots. Examples would be something like: 'Christmas 2022', or 'End-of-year report 2021'. By default this is filled with 'Snapshot \{sdts\}', holding the respective sdts. |
| is_hourly        | boolean   | Captures if the time of a sdts is on an exact hour, meaning minutes=0 and seconds=0. All sdts created by this macro are daily and therefore always hourly, but this column enables future inserts of custom, user-defined sdts. |
| is_daily         | boolean   | Captures if the time of a sdts is on exactly midnight, meaning hours=0, minutes=0 and seconds=0. This depends on your desired daily_snapshot_time, but is not used by the downstream macros, and just generates additional metadata for potential future use. |
| is_weekly        | boolean   | Captures if the day of the week of a sdts is Monday. |
| is_monthly       | boolean   | Captures if a sdts is the first day of a month. |
| is_end_of_month  | boolean   | Captures if a sdts is the last day of a month. |
| is_quarterly     | boolean   | Captures if a sdts is the first day of a quarter. |
| is_yearly        | boolean   | Captures if a sdts is the first day of a year. |
| is_end_of_year   | boolean   | Captures if a sdts is the last day of a year. |
| comment          | string    | Allows users to write custom comments for each sdts. By default this column is set to NULL. |

| Parameters          | Data Type | Required  | Default Value              | Explanation |
|---------------------|-----------|-----------|----------------------------|-------------|
| start_date          | date      | mandatory | –                          | Defines the earliest date that should be available inside the snapshot_table. The format of this date must be 'YYYY-MM-DD'. |
| daily_snapshot_time | time      | mandatory | –                          | Defines the time that your daily snapshots should have. Usually this is either something right before daily business starts, or after daily business is over. Needs to be in the format 'hh:mm:ss'. |
| sdts_alias          | string    | optional  | datavault4dbt.sdts_alias   | Defines the name of the snapshot date timestamp column inside the snapshot_table. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
start_date: '2015-01-01'
daily_snapshot_time: '07:30:00'
{%- endset -%}    

{{ datavault4dbt.control_snap_v0(yaml_metadata=yaml_metadata) }}   
```

### DESCRIPTION

- **start_date**: The parameter start_date has to be in the format ‘YYYY-MM-DD’. With the start_date set to ‘2015-01-01’, the snapshot table would hold daily snapshots beginning at 2015.
- **daily_snapshot_time**: ’07:30:00′ The snapshots inside this table would all have the time ’07:30:00′. Must be set in the format ‘hh:mm:ss’.