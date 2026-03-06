---
sidebar_position: 6
sidebar_label: Standard Hub
title: Standard Hub
---

# STANDARD HUB

---

This macro creates a standard Hub entity based on one or more stage models. The macro requires an input source model similar to the output of the stage macro. So by default the stage models would be used as source models for hubs. If a Hub is loaded by multiple sources, each source needs to have the same number of Business Key columns. Additionally, a multi-source hub needs a [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) defined for each source.

Features:

- Loadable by multiple sources
- Supports multiple updates per batch and therefore initial loading
- Using a dynamic high-water-mark to optimize loading performance of multiple loads
- Allows source mappings for deviations between source column names and hub column names

### REQUIRED PARAMETERS

| Parameter      | Data Type                                    | Required  | Default Value | Explanation |
|----------------|----------------------------------------------|-----------|---------------|-------------|
| hashkey        | string                                       | mandatory | –             | Name of the hashkey column inside the stage, that should be used as PK of the Hub. |
| business_keys  | string \| list of strings                    | mandatory | –             | Name(s) of the business key columns that should be loaded into the hub and are the input of the hashkey column. Needs to be available inside the stage model. If the names differ between multiple sources, you should define here how the business keys should be called inside the final hub model. The actual input column names need to be defined inside the `source_model` parameter then. |
| source_models  | string \| list of dictionaries \| dictionary | mandatory | –             | If single source, just a string holding the name of the stage model is required. For multi source Hubs, a list of dictionaries with information about each source is required. For more information see [this](/docs/General_Usage_Notes/Multi-Source_Entities/) page! The inner dictionaries need to have `name` as a key, and optionally the keys `rsrc_static`, `hk_column` and `bk_columns`. For further information about the `rsrc_static` attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) |

### OPTIONAL PARAMETERS

| Parameter          | Data Type                 | Required | Default Value            | Explanation |
|--------------------|---------------------------|----------|--------------------------|-------------|
| disable_hwm        | boolean                   | optional | False                    | Whether the automatic application of a High-Water Mark (HWM) should be disabled or not. |
| src_ldts           | string                    | optional | datavault4dbt.ldts_alias | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc           | string                    | optional | datavault4dbt.rsrc_alias | Name of the rsrc column inside the source models. Is optional, will use the global variable `datavault4dbt.rsrc_alias`. Needs to use the same column name as defined as alias inside the staging model. |
| additional_columns | string \| list of strings | optional | none                     | Column or list of columns that will additionally be added to the hub. (Available from v1.12.0) |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_account_h'
business_keys: 
    - account_key
    - account_number
source_models: stage_account
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

- **hashkey:** This hashkey column was created before inside the corresponding staging area, using the stage macro.
- **business_keys:** This hub has two business keys which are both defined here. Need to equal the input columns for the hashkey column.
- **source_models:** This would create a hub loaded from only one source, which is not uncommon. It uses the model `stage_account` and since no `bk_columns` are specified, the same columns as defined in `business_keys` will be selected from the source.
  - The `rsrc_static` attribute is not set, because it is not required for single source entities. For more information see [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute).