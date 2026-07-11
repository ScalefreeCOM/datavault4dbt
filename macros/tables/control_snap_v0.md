{% docs control_snap_v0 %}

## Control Snapshot v0

Creates a snapshot table to control snapshot-based tables like PITs and Bridges. The snapshot table will hold
daily snapshots starting at a specific start_date with a configurable daytime. Usually one snapshot table per
Data Vault environment is created. The model needs to be scheduled daily at the time matching the desired
snapshot time.

In addition to the actual snapshot datetimestamp (sdts), the macro generates the following metadata columns:
`replacement_sdts`, `caption`, `is_hourly`, `is_daily`, `is_beginning_of_week`, `is_end_of_week`,
`is_beginning_of_month`, `is_end_of_month`, `is_beginning_of_quarter`, `is_end_of_quarter`,
`is_beginning_of_year`, `is_end_of_year`, `comment`, `force_active`.

### Usage

```jinja
{{ datavault4dbt.control_snap_v0(
    start_date='2020-01-01T00-00-00',
    daily_snapshot_time='07:00:00'
) }}
```

### Metadata block usage

```jinja
{{ datavault4dbt.control_snap_v0(yaml_metadata=meta) }}
```

{% enddocs %}
