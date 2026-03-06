---
sidebar_position: 8
sidebar_label: Non-Historized Link
title: Non-Historized Link
---

# NON-HISTORIZED LINK

---

This macro creates a non-historized (former transactional) link entity, connecting two or more entities, or an entity with itself. It can be loaded by one or more source staging tables, if multiple sources share the same business definitions. If multiple sources are used, it is required that they all have the same number of foreign keys inside, otherwise they would not share the same business definition of that non-historized link. Additionally, a multi-source nh-link needs a [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) defined for each source.

In the background a non-historized link uses exactly the same loading logic as a regular link, but adds the descriptive attributes as additional payload.

Features:

- Loadable by multiple sources
- Supports multiple updates per batch and therefore initial loading
- Using a dynamic high-water-mark to optimize loading performance of multiple loads
- Allows source mappings for deviations between source column names and nh-link column names

### REQUIRED PARAMETERS

| Parameters     | Data Type                                   | Required  | Default Value | Explanation |
|----------------|---------------------------------------------|-----------|---------------|-------------|
| link_hashkey   | string                                      | mandatory | –             | Name of the non-historized link hashkey column inside the stage. Should be calculated out of all business keys inside the link. |
| payload        | list of strings                             | mandatory | –             | A list of all the descriptive attributes that should be the payload of this non-historized link. If the names differ between source models, this list will define how the columns are named inside the result non historized link. The mapping which columns to use from which source model then need to be defined inside the parameter `payload` inside the variable `source_models`. |
| source_models  | string \| list of dictionaries \| dictionary | mandatory | –             | For a single source entity, a string with the name of the source staging model is required. For multi source entities, a list of dictionaries with information for each source is required. These dictionaries need to have the key `name`, and optionally the keys `rsrc_static`, `hk_column`, `fk_columns` and `payload`. Especially regarding multi-source metadata, please see this page! For further information about the rsrc_static attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute) |

### OPTIONAL PARAMETERS

| Parameters             | Data Type                | Required     | Default Value              | Explanation |
|------------------------|--------------------------|--------------|----------------------------|-------------|
| foreign_hashkeys       | list of strings          | recommended  | –                          | List of all hashkey columns inside the non-historized link that refer to other hub entities. All hashkey columns must be available inside the stage area. If specified, this list can be empty or contain one or more foreign hashkeys. |
| disable_hwm            | boolean                  | optional     | False                      | Whether the automatic application of a High-Water Mark (HWM) should be disabled or not. |
| source_is_single_batch | boolean                  | optional     | False                      | See detailed explanation below. |
| src_ldts               | string                   | optional     | datavault4dbt.ldts_alias   | Name of the ldts column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| src_rsrc               | string                   | optional     | datavault4dbt.rsrc_alias   | Name of the rsrc column inside the source models. Needs to use the same column name as defined as alias inside the staging model. |
| union_strategy         | string                   | optional     | ALL                        | Defines how multiple sources should be unionized. ALL will result in a UNION ALL and represents the default value. Should only be changed if you have duplicates across source systems and do not want to deduplicate them upfront. Possible values: ALL or DISTINCT. |
| additional_columns     | string \| list of strings | optional     | none                       | Column or list of columns that will be additionally be added to the non-historized link. (Available from v1.12.0) |

## EXAMPLE 1

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_creditcard_transactions_nl'
foreign_hashkeys: 
    - 'hk_creditcard_h'
payload:
    - transactionid
    - amount
    - currency_code
    - is_canceled
    - transaction_date  
source_models: stage_creditcard_transactions
{%- endset -%}    

{{ datavault4dbt.nh_link(yaml_metadata=yaml_metadata) }}
```

## EXAMPLE 2

```jinja
{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_creditcard_transactions_nl'
foreign_hashkeys: 
    - 'hk_creditcard_h'
payload:
    - transactionid
    - amount
    - currency_code
    - is_canceled
    - transaction_date
source_models:
    - name: stage_creditcard_transactions
      rsrc_static: '*/VISA/Transactions/*'
    - name: stage_purchases
      link_hk: 'transaction_hashkey'
      fk_columns: ['creditcard_hkey']
      payload: 
        - id
        - amount_CUR
        - currency
        - status_flag
        - date
      rsrc_static: '*/SHOP/Creditcard_Purchases/*'
{%- endset -%}    

{{ datavault4dbt.nh_link(yaml_metadata=yaml_metadata) }}
```

### DESCRIPTION

With this example, a non-historized link is created.

- **link_hashkey:**
  - hk_creditcard_transactions_nl: This hashkey column belongs to the non-historized link between credit card and transaction, and was created at the staging layer by the stage macro.
- **foreign_hashkeys:**
  - [`hk_creditcard_h`] List of all hashkey columns inside the non-historized link, that refer to other hub entities. All hashkey columns must be available inside the stage area. This example contains only one foreign hashkey. As a result, there is only one hub connected to the non-historized link.
- **source_models:**
  - This would create a link loaded from two sources, which is not uncommon. It uses the models `stage_creditcard_transactions` and `stage_purchases`. For further information about the rsrc_static attribute, please visit the following page: [rsrc_static Attribute](/docs/General_Usage_Notes/The_rsrc_static_Attribute)

### PERFORMANCE BOOSTING

Especially Non-Historized Links tend to have a high volume of data. In such cases, you might want to boost the loading performance of your Non-Historized Links. With the datavault4dbt macro you have two options for this, but be aware that they **should be used with caution and only when your data meets specific requirements** (more an that later):

- Disabling the High-Water Mark for the Loading Process
- Disabling the Deduplication of the source data

Both steps would strongly reduce complexity of the SQL used for the loading process and therefore increase the performance.

**But to ensure your NH Link data is still valid, the stage used for the NH Link must meet specific requirements!:**

#### DISABLING DEDUPLICATION OF THE SOURCE DATA

To safely disable the deduplication, you have to verify that the underlying staging model of the NH Link only holds one row per Link Hashkey.

We highly recommend setting up a uniqueness test within a yml file, covering the hashkey column of the staging model.

Once this is guaranteed, you just add the following parameter to your macro call:

```jinja
source_is_single_batch = true
```

If this is set to true, this QUALIFY statement:

```jinja
earliest_hk_over_all_sources AS (
{# Deduplicate the unionized records again to only insert the earliest one. #}

    SELECT
        lcte.*
    FROM {{ ns.last_cte }} AS lcte

    QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ link_hashkey }} ORDER BY {{ src_ldts }}) = 1

    {%- set ns.last_cte = 'earliest_hk_over_all_sources' -%}

),
```

will note be activated!

To continuously ensure data validity within your NH Link, we also recommend setting up a uniqueness test across the Link Hashkey on your NH Links.

#### DISABLING HIGH-WATER MARK

The High-Water Mark can be disabled safely, but typically would decrease the performance again.

We recommend to **try a bit what works best in your environment**. You basically have three options:

- **Keep HWM activated**, for multi-source NH Links this would require the [rsrc_static](/docs/General_Usage_Notes/The_rsrc_static_Attribute) to be defined for each source. For single source NH Links, nothing needs to be done, the HWM is activated automatically.
- **Disable the HWM entirely.** For multi-source NH Links you just need to not specify the rsrc_static attribute. For single-source NH Links you need to add the parameter disable_hwm=true to your NH Link macro call.
- **Move the HWM to a previous layer.** First, you apply the previous step to disable the HWM in the NH Link. Then you implement some kind of mechanism in previous dbt layers to ensure that only records newer than what you already processed are available there. This could be especially effective when combining with different materializations of these previous layers.