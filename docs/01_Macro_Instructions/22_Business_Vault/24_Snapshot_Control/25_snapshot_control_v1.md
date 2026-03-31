---
sidebar_position: 25
sidebar_label: Snapshot Control v1
title: Snapshot Control v1
---

# SNAPSHOT CONTROL V1

---

This macro creates a view that extends an existing control_snap_v0 table by dynamically changing information. These information include a logic to implement logarithmic snapshots. That means that the further I look into the past, the more coarsely the snapshots should be granulated.

For example, I want to keep daily snapshots for the past 30 days, but I am not interested in daily snapshots for the past 10 years, and therefore I only keep weekly snapshots for the past 6 months, monthly snapshots for the past 3 years, and yearly snapshots forever. This procedure strongly reduces the number of active snapshots, and therefore also the number of rows, and the required computation inside all PITs and Bridges. This logic is optional and would be captured in a boolean column called `is_active`.

Whenever a logarithmic snapshot logic is used and picked up by PIT tables, a logic is required that deletes records out of PIT tables, that are no longer active. For this, a post_hook called “clean_up_pit” is provided in this package, that should be applied for each PIT table.

In addition to that, a few other dynamic columns are generated:

| Column               | Data Type | Explanation |
|----------------------|-----------|-------------|
| is_latest            | boolean   | Captures if a sdts is the latest one inside the snapshot table. There is always only one snapshot inside the view that has TRUE here. |
| is_current_year      | boolean   | Captures if a sdts is part of the current calendar year. |
| is_last_year         | boolean   | Captures if a sdts is part of the last calendar year. |
| is_rolling_year      | boolean   | Captures if a sdts is inside the past year, starting from the current date. |
| is_last_rolling_year | boolean   | Captures if a sdts is inside the range that starts two years ago (from the current date) and ranges until one year ago (from the current date). |

| Parameters      | Data Type | Required  | Default Value | Explanation |
|----------------|-----------|-----------|---------------|-------------|
| control_snap_v0 | string    | mandatory | –             | The name of the underlying version 0 control snapshot table. Needs to be available as a dbt model. |

| Parameters | Data Type  | Required | Default Value            | Explanation |
|------------|-----------|----------|--------------------------|-------------|
| log_logic  | dictionary | optional | None                     | Defining the desired durations of each granularity. Available granularities are 'daily', 'weekly', 'monthly', 'end_of_month', 'quarterly', 'yearly' and 'end_of_year'. For each granularity the duration can be defined as an integer and the time unit for that duration. The units include (in BigQuery): DAY, WEEK, MONTH, QUARTER, YEAR. Besides defining a duration and a unit for each granularity, there is also the option to set a granularity to 'forever'. E.g. reporting requires daily snapshots for 3 months, and after that the monthly snapshots should be kept forever. If log_logic is not set, no logic will be applied, and all snapshots will stay active. The other dynamic columns are calculated anyway. The duration is always counted from the current date. |
| sdts_alias | string     | optional | datavault4dbt.sdts_alias | Defines the name of the snapshot date timestamp column inside the snapshot_table. |

## EXAMPLE 1

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
control_snap_v0: 'control_snap_v0'
log_logic: 
    daily:
        duration: 3
        unit: 'MONTH'
    weekly:
        duration: 1
        unit: 'YEAR'
    monthly:
        duration: 5
        unit: 'YEAR'
    yearly:
        forever: true
{%- endset -%}    

{{ datavault4dbt.control_snap_v1(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **control_snap_v0**:
  - __control_snap_v0__: The name of the underlying version 0 control snapshot table.
- **log_logic**: For the selectable options of the granularity of the snapshots and the units of maintaining the snapshots, please look at the table above.
  - **daily**: Daily snapshots are kept 3 months.
  - **weekly**: Weekly snapshots are kept 1 years.
  - **monthly**: Monthly snapshots are kept 5 years.
  - **yearly**: Yearly snapshots are kept forever.