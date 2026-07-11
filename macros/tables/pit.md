{% docs pit %}

## Point-in-Time (PIT) Table

Creates a PIT table to gather snapshot-based information about one hub and its surrounding satellites.
For this macro to work, a snapshot table is required that has a trigger column to identify which snapshots
to include in the PIT table. The easiest way to create such a snapshot table is to use the control_snap macros
provided by this package.

Features:
- Tracks the active satellite entries for each hub entry at each snapshot.
- Strongly improves performance if upstream queries require many JOIN operations.
- Creates a unique dimension key to optimize loading performance of incremental loads.
- Allows inserting a static string as record source column, matching business vault definition.

### Usage

```jinja
{{ datavault4dbt.pit(
    tracked_entity='account_h',
    hashkey='hk_account_h',
    sat_names=[
        'account_data_sfdc_1_s',
        'account_financials_erp_1_s'
    ],
    snapshot_relation='control_snap_v1',
    dimension_key='hk_account_h_d'
) }}
```

### Metadata block usage

```jinja
{{ datavault4dbt.pit(yaml_metadata=meta) }}
```

{% enddocs %}
