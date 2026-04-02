---
sidebar_position: 25
sidebar_label: Snapshot Control v1
title: Snapshot Control v1
---

# SNAPSHOT CONTROL V1

---

This macro creates a view that extends an existing `control_snap_v0` table by dynamically adding information. This information includes a logic to implement logarithmic snapshots. That means that the further I look into the past, the more coarsely the snapshots should be granulated.

For example, I want to keep daily snapshots for the past 30 days, but I am not interested in daily snapshots for the past 10 years, and therefore I only keep weekly snapshots for the past 6 months, monthly snapshots for the past 3 years, and yearly snapshots forever. This procedure strongly reduces the number of active snapshots, and therefore also the number of rows, and the required computation inside all PITs and Bridges. This logic is optional and would be captured in a boolean column called `is_active`.

Whenever a logarithmic snapshot logic is used and picked up by PIT tables, a logic is required that deletes records out of PIT tables that are no longer active. For this, a post_hook called `clean_up_pit` is provided in this package, that should be applied for each PIT table.

In addition to inheriting the core columns (`sdts`, `replacement_sdts`, `caption`, `is_hourly`, `is_daily`, `comment`) from the `control_snap_v0` view, the following dynamic and pass-through columns are generated:

| Column | Data Type | Explanation |
|---|---|---|
| snapshot_trigger_column | boolean | Defaults to `is_active`. Captures if a snapshot is considered "active" based on the logarithmic logic provided. If no logic is provided, this defaults to TRUE for all snapshots. |
| is_latest | boolean | Captures if a sdts is the latest one inside the snapshot table. There is always exactly one snapshot inside the view that evaluates to TRUE here. |
| is_beginning_of_week | boolean | Inherited from v0. Captures if a sdts is the first day of the week based on your global config. |
| is_end_of_week | boolean | Inherited from v0. Captures if a sdts is the last day of the week. |
| is_beginning_of_month | boolean | Inherited from v0. Captures if a sdts is the first day of a month. |
| is_end_of_month | boolean | Inherited from v0. Captures if a sdts is the last day of a month. |
| is_beginning_of_quarter | boolean | Inherited from v0. Captures if a sdts is the first day of a quarter. |
| is_end_of_quarter | boolean | Inherited from v0. Captures if a sdts is the last day of a quarter. |
| is_beginning_of_year | boolean | Inherited from v0. Captures if a sdts is the first day of a year. |
| is_end_of_year | boolean | Inherited from v0. Captures if a sdts is the last day of a year. |
| is_current_year | boolean | Captures if a sdts is part of the current calendar year. |
| is_last_year | boolean | Captures if a sdts is part of the last calendar year. |
| is_rolling_year | boolean | Captures if a sdts is inside the past year, starting from the current date. |
| is_last_rolling_year | boolean | Captures if a sdts is inside the range that starts two years ago (from the current date) and ranges until one year ago (from the current date). |

### Macro Parameters

| Parameters | Data Type | Required | Default Value | Explanation |
|---|---|---|---|---|
| log_logic  | dictionary or list | optional | None                     | Defining the desired durations of each granularity. Available granularities are `daily`, `weekly`, `monthly`, `yearly`. For each granularity the duration can be defined as an integer and the time unit for that duration. The units include (in BigQuery): DAY, WEEK, MONTH, QUARTER, YEAR. Besides defining a duration and a unit for each granularity, there is also the option to set a granularity to 'forever'. E.g. reporting requires daily snapshots for 3 months, and after that the monthly snapshots should be kept forever. If log_logic is not set, no logic will be applied, and all snapshots will stay active. The other dynamic columns are calculated anyway. The duration is always counted from the current date. It is also possible to provide a list of dictionaries to support multiple snapshot trigger columns. |
| sdts_alias | string | optional | datavault4dbt.sdts_alias | Defines the name of the snapshot date timestamp column inside the snapshot_table. |


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

## EXAMPLE 2 (Multiple Logics - Snowflake only)

```jinja
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
control_snap_v0: 'control_snap_v0'
log_logic: 
    - is_active_monthly:
        monthly:
            duration: forever
    - is_active_yearly:
        yearly:
            duration: forever
{%- endset -%}

{{ datavault4dbt.control_snap_v1(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **control_snap_v0**:
  - __control_snap_v0__: The name of the underlying version 0 control snapshot table.
- **log_logic**: Here we define two different active logics for the snapshots, which will create two separate columns:
  - **is_active_1**: Monthly snapshots are kept 1 year.
  - **is_active_2**: Weekly snapshots are kept 2 months.