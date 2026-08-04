{% docs control_snap_v1 %}

## Control Snapshot v1

Creates a view that extends an existing control_snap_v0 table by dynamically applying logarithmic snapshot logic.
The further you look into the past, the more coarsely the snapshots are granulated — for example, keeping daily
snapshots for the past 30 days but only weekly snapshots for the past 6 months and monthly snapshots for the
past 3 years.

This procedure strongly reduces the number of active snapshots and therefore the computation inside all PITs
and Bridges.

In addition to the logarithmic `is_active` column, the following dynamic columns are generated:
`is_latest`, `is_current_year`, `is_last_year`, `is_rolling_year`, `is_last_rolling_year`.

Note: Whenever a logarithmic snapshot logic is used with PIT tables, the `clean_up_pit` post-hook must be
applied to each PIT table to remove inactive records.

### Usage

```jinja
{% raw %}
{{ datavault4dbt.control_snap_v1(
    control_snap_v0='control_snap_v0',
    log_logic={
        'daily':   {'duration': 3, 'unit': 'MONTH'},
        'weekly':  {'duration': 1, 'unit': 'YEAR'},
        'monthly': {'duration': 5, 'unit': 'YEAR'},
        'yearly':  {'forever': 'TRUE'}
    }
) }}
{% endraw %}
```

### Metadata block usage

`meta` is a Jinja string variable containing all macro parameters as YAML. Define it using a
`set` block in the model SQL file — the macro parses the YAML string at runtime:

```jinja
{% raw %}
{%- set meta -%}
control_snap_v0: 'control_snap_v0'
log_logic:
  daily:
    duration: 3
    unit: MONTH
  weekly:
    duration: 1
    unit: YEAR
  monthly:
    duration: 5
    unit: YEAR
  yearly:
    forever: 'TRUE'
{%- endset -%}

{{ datavault4dbt.control_snap_v1(yaml_metadata=meta) }}
{% endraw %}
```

{% enddocs %}
