{% docs clean_up_pit %}

## Clean Up PIT (Post-Hook)

This macro should be used as a post-hook for each PIT table whenever a logarithmic snapshot logic is used.
The macro deletes all records in a PIT table that are no longer active according to the snapshot view.
Deletion is safe here because no actual data is deleted — only pointers to satellite entries.

### Usage as a post-hook

```jinja
{{ config(
    post_hook="{{ datavault4dbt.clean_up_pit('control_snap_v1') }}"
) }}
```

### With custom column names

```jinja
{{ config(
    post_hook="{{ datavault4dbt.clean_up_pit(
        snapshot_relation='control_snap_v1',
        snapshot_trigger_column='is_active',
        sdts='sdts'
    ) }}"
) }}
```

{% enddocs %}
