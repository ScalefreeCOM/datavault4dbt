---
sidebar_position: 22
sidebar_label: PIT
title: PIT
---

# PIT

---

This macro creates a PIT table to gather snapshot based information of one hub and its surrounding satellites. For this macro to work, a snapshot table is required, that has a trigger column to identify which snapshots to include in the PIT table. The easiest way to create such a snapshot table is to use the control_snap macros provided by this package.

Features:

- Tracks the active satellite entries for each entry in a Hub for each snapshot
- Strongly improves performance if upstream queries requires many JOIN operations
- Creates a unique dimension key to optimize loading performance of incremental loads
- Allows to insert a static string as record source column, matching business vault definition of a record source

### REQUIRED PARAMETERS

| Parameters       | Data Type       | Required  | Default Value | Explanation |
|------------------|----------------|-----------|---------------|-------------|
| tracked_entity   | string         | mandatory | –             | Name of the tracked Hub entity. Must be available as a model inside the dbt project. |
| hashkey          | string         | mandatory | –             | The name of the hashkey column inside the previously referred Hub entity. |
| sat_names        | list of strings| mandatory | –             | A list of all the satellites that should be included in this PIT table. Can only be satellites that are attached to the tracked Hub, and should typically include all those satellites. You should always refer here to the version 1 satellites, since those hold the load-end-date. The macro currently supports regular satellites and nh-satellites. |
| snapshot_relation| string         | mandatory | –             | The name of the snapshot relation. It needs to be available as a model inside this dbt project. |
| dimension_key    | string         | mandatory | –             | The desired name of the dimension key inside the PIT table. Should follow naming conventions. Recommended is the name of the hashkey with a `_d` suffix. |

### OPTIONAL PARAMETERS

| Parameters              | Data Type | Required  | Default Value                 | Explanation |
|-------------------------|-----------|-----------|-------------------------------|-------------|
| pit_type                | string    | optional  | None                          | String to insert into the `pit_type` column. Has to be prefixed by ''. Allows for future implementations of other PIT variants, like T-PITs etc. Can be set freely, something like `PIT` could be the default. |
| snapshot_trigger_column | string    | important | None                          | The name of the column inside the previously mentioned snapshot relation, that is boolean and identifies the snapshots that should be included in the PIT table. |
| ldts                    | string    | optional  | datavault4dbt.ldts_alias      | Name of the ldts column inside all source models. Needs to use the same column name as defined as alias inside the staging model. |
| custom_rsrc             | string    | optional  | None                          | A custom string that should be inserted into the `rsrc` column inside the PIT table. Since a PIT table is a business vault entity, the technical record source is no longer used here. |
| ledts                   | string    | optional  | datavault4dbt.ledts_alias     | Name of the load-end-date column inside the satellites. |
| sdts                    | string    | optional  | datavault4dbt.sdts_alias      | Name of the snapshot date timestamp column inside the snapshot table. Set here. |
| snapshot_optimization   | boolean   | optional  | false                         | Available from v1.15.0. If set to `True`, and if the model is run in incremental mode, only the relevant snapshots (i.e. those that are newer or equal because of late arriving data) than the max sdts in the existing PIT table will be considered. This can significantly improve performance of incremental loads on large snapshot tables. If set to `True`, the model needs to be configured with a unique_key constraint as there may be updates due to late arriving data. Affected Adapters: Snowflake only! |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental',
        post_hook="{{ datavault4dbt.clean_up_pit('control_snap_v1') }}") }}

{%- set yaml_metadata -%}
pit_type: '!Regular PIT'
tracked_entity: 'account_h'
hashkey: 'hk_account_h'
sat_names:
    - account_lroc_p_s
    - account_lroc_n_s
    - account_hroc_p_s
    - account_hroc_n_s
snapshot_relation: 'control_snap_v1'
snapshot_trigger_column: 'is_active'
dimension_key: 'hk_account_d'
custom_rsrc: 'PIT table for SAP/Accounts. For more information see our Website!'
{%- endset -%}    

{{ datavault4dbt.pit(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a PIT Table is created. In line three of this example, the post hook “clean_up_pit” is used. For further information about the hook, click on the following link: [Hook Clean Up PITs](/docs/Macro_Instructions/Business_Vault/PIT/hook_cleanup_pits/)

- **pit_type**:
  - __!Regular PIT__: PIT type is set to Regular PIT. Optional.
- **tracked_entity**:
  - __account_h__: This PIT table tracks the Hub Account.
- **hashkey**:
  - __hk_account_h__: The name of the hashkey column (`hk_account_h`) inside the previously referred Hub entity (`account_h`).
- **sat_names**:
  - [`account_lroc_p_s`, `account_lroc_n_s`, `account_hroc_p_s`, `account_hroc_n_s`]: This four satellites are included in the PIT table.
- **snapshot_relation**:
  - __control_snap_v1__: The name of the snapshot relation.
- **snapshot_trigger_column**:
  - __is_active__: The name of the column inside the previously mentioned snapshot relation that is boolean and identifies the snapshots that should be included in the PIT table.
- **dimension_key**:
  - __hk_account_d__: The desired name of the dimension key inside the PIT table.
- **custom_rsrc**:
  - __PIT table for SAP/Accounts.__: A custom string that should be inserted into the `rsrc` column inside the PIT table. Optional.