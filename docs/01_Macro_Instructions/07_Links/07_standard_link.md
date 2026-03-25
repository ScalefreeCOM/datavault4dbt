---
sidebar_position: 7
sidebar_label: Standard Link
title: Standard Link
---

# STANDARD LINK

---

This macro creates a link entity, connecting two or more entities, or an entity with itself. It can be loaded by one or more source staging tables, if multiple sources share the same business definitions. Typically a link would only be loaded by multiple sources, if those multiple sources also share the business definitions of the hubs, and therefore load the connected hubs together as well. If multiple sources are used, it is required that they all have the same number of foreign keys inside, otherwise they would not share the same business definition of that link. Additionally, a multi-source link needs a [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) defined for each source.

Features:

- Loadable by multiple sources
- Supports multiple updates per batch and therefore initial loading
- Using a dynamic high-water-mark to optimize loading performance of multiple loads
- Allows source mappings for deviations between source column names and hub column names

### REQUIRED PARAMETERS

| Parameters        | Data Type                                    | Required  | Default Value | Explanation |
|-------------------|----------------------------------------------|-----------|---------------|-------------|
| link_hashkey      | string                                       | mandatory | –             | Name of the link hashkey column inside the stage. Should get calculated out of all business keys inside the link. |
| foreign_hashkeys  | list of strings                              | mandatory | –             | List of all hashkey columns inside the link, that refer to other hub entities. All hashkey columns must be available inside the stage area. |
| source_models     | string \| list of dictionaries \| dictionary | mandatory | –             | For a single source entity, a string with the name of the source staging model is required. For multi source entities, a list of dictionaries with information about the source models is required. For more information see [this](/docs/General_Usage_Notes/Multi-Source_Entities/) page! The dictionaries need to have the keys `name` and optionally the keys `rsrc_static`, `hk_column` and `fk_columns`. For further information about the `rsrc_static` attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) |

### OPTIONAL PARAMETERS

| Parameters         | Data Type                 | Required | Default Value             | Explanation |
|--------------------|---------------------------|----------|---------------------------|-------------|
| disable_hwm        | boolean                   | optional | False                     | Whether the automatic application of a High-Water Mark (HWM) should be disabled or not. |
| src_ldts           | string                    | optional | datavault4dbt.ldts_alias  | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc           | string                    | optional | datavault4dbt.rsrc_alias  | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| additional_columns | string \| list of strings | optional | none                      | Column or list of columns that will additionally be added to the link. |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_opportunity_account_l'
foreign_hashkeys: 
    - 'hk_opportunity_h'
    - 'hk_account_h'
source_models: stage_opportunity
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a regular standard link is created. The created link represents a single source link because there is only one underlying source model defined in the metadata.

- **link_hashkey:**
  - hk_opportunity_account_l: This hashkey column belongs to the link between opportunity and account, and was created at the staging layer by the stage macro.
- **foreign_hashkeys:**
  - [`hk_opportunity_h`, `hk_account_h`] The link between opportunity and account needs to contain both the hashkey of account and contact to enable joins the the corresponding hub entities.
- **source_models:**
  - This would create a link loaded from only one source, which is not uncommon. It uses the model `stage_account`. The rsrc_static attribute is not set, because it is not required for single source entities. For further information about the rsrc_static attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute).

## EXAMPLE 2

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_opportunity_account_l'
foreign_hashkeys: 
    - 'hk_opportunity_h'
    - 'hk_account_h'
source_models:
    - name: stage_opportunity
      rsrc_static: '*/SALESFORCE/Opportunity/*'
    - name: stage_account
      rsrc_static: '*/SAP/Account/*'
      link_hk: 'hashkey_account_opportunity'
      fk_columns: 
          - hashkey_opportunity
          - hashkey_account
{%- endset -%}    

{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a regular standard link is created. The created link represents a multi source link because there are multiple underlying source models defined.

- **link_hashkey:**
  - hk_opportunity_account_l: This hashkey column belongs to the link between opportunity and account, and was created at the staging layer by the stage macro.
- **foreign_hashkeys:**
  - [`hk_opportunity_h`, `hk_account_h`] The link between opportunity and account needs to contain both the hashkey of account and contact to enable joins the the corresponding hub entities.
- **source_models:**
  - This would create a link loaded from two sources, which is also not uncommon. With “link_hk” and “fk_columns” defined differently for stage_account, a source mapping is enabled, that allows users to use different input columns for different source models.
  - For further information about the rsrc_static attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute).

### DISABLING HIGH-WATER MARK

The High-Water Mark can be disabled safely, but typically would decrease the performance again.

We recommend to **try a bit what works best in your environment**. You basically have three options:

- **Keep HWM activated**, for multi-source Links this would require the [rsrc_static](/docs/General_Usage_Notes/The_rsrc_static_Attribute) to be defined for each source. For single source Links, nothing needs to be done, the HWM is activated automatically.
- **Disable the HWM entirely.** For multi-source Links you just need to not specify the rsrc_static attribute. For single-source Links you need to add the parameter disable_hwm=true to your Link macro call.
- **Move the HWM to a previous layer.** First, you apply the previous step to disable the HWM in the Links. Then you implement some kind of mechanism in previous dbt layers to ensure that only records newer than what you already processed are available there. This could be especially effective when combining with different materializations of these previous layers.