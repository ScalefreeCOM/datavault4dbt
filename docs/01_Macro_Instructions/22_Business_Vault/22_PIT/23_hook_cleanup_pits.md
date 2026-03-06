---
sidebar_position: 23
sidebar_label: Hook Cleanup PITs
title: Hook Cleanup PITs
---

# HOOK CLEANUP PITS

---

This macro should be used as a post hook for each PIT table, whenever a logarithmic snapshot logic is used. The macro deletes all records in a PIT table, that are no longer active. Deletion is no problem here because no actual data is deleted, only pointers to satellite entries.

| Parameters               | Data Type | Required  | Default Value                         | Explanation |
|--------------------------|-----------|-----------|---------------------------------------|-------------|
| snapshot_relation        | string    | mandatory | –                                     | The name of the dbt model that creates the snapshot table / view, that has the logarithmic snapshot logic applied. |
| snapshot_trigger_column  | string    | optional  | datavault4dbt.snapshot_trigger_column | The name of the boolean column inside the snapshot tables, that activate/deactivate single snapshots. |
| sdts                     | string    | optional  | datavault4dbt.sdts_alias              | The name of the snapshot date timestamp column inside the snapshot table. |

Example Usage:

An example usage for applying this macro as a post hook for a PIT table would look like this inside the PIT source_models config block:

```jinja
{{ config(post_hook="{{ datavault4dbt.clean_up_pit('control_snap_view') }}") }}
```